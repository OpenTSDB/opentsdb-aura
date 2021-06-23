/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  Yahoo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.aura.metrics;

import net.opentsdb.aura.metrics.meta.AnyFilter;
import net.opentsdb.aura.metrics.meta.Filter;
import net.opentsdb.aura.metrics.meta.LiteralFilter;
import net.opentsdb.aura.metrics.meta.MetaQueryBuilder;
import net.opentsdb.aura.metrics.meta.MetricFilter;
import net.opentsdb.aura.metrics.meta.RegexpFilter;
import net.opentsdb.aura.metrics.meta.TagKeyFilter;
import net.opentsdb.query.filter.AnyFieldRegexFilter;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.MetricRegexFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagKeyLiteralOrFilter;
import net.opentsdb.query.filter.TagKeyRegexFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import net.opentsdb.aura.metrics.meta.Filter.Operator;
import net.opentsdb.aura.metrics.meta.Filter.Type;

public class MetaQueryBuilderTSDBExt extends MetaQueryBuilder {

  public static MetaQueryBuilderTSDBExt newBuilder() {
    return new MetaQueryBuilderTSDBExt();
  }

  public MetaQueryBuilder fromTSDBQueryFilter(QueryFilter tsdbQueryFilter) {
    if (tsdbQueryFilter != null) {
      nestedFilter = buildFromTsdbFilter(tsdbQueryFilter, null);
    }
    return this;
  }


  Filter buildFromTsdbFilter(
      final QueryFilter tsdbQueryFilter,
      final Operator operator) {
    /** ------- Explicit tags ------- */
    if (tsdbQueryFilter instanceof ExplicitTagsFilter) {
      return buildFromTsdbFilter(
          ((ExplicitTagsFilter) tsdbQueryFilter).getFilter(), null);
    /** ------- Chain ------- */
    } else if (tsdbQueryFilter instanceof ChainFilter) {
      ChainFilter chainFilter = (ChainFilter) tsdbQueryFilter;
      List<QueryFilter> queryFilters = chainFilter.getFilters();
      ChainFilter.FilterOp op = chainFilter.getOp();

      Filter.Operator chainOperator =
          Filter.Operator.valueOf(Filter.Operator.class, op.toString());
      List<Filter> chain =
          queryFilters.stream()
              .map(filter -> buildFromTsdbFilter(filter, chainOperator))
              .collect(Collectors.toList());
      
      // Nots are a "fun" case. If we have a chain like:
      // "NOT .*foo AND a.*" then we ACTUALLY want the NOT after the positive
      // matches so we can sort the list. OR's it doesn't matter, we'd get every-
      // thing in that case.
      if (op == ChainFilter.FilterOp.AND) {
        chain.sort(FILTER_COMPARATOR);
      }
      return net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
          .addChain(chain)
          .withOperator(operator)
          .build();
    /** ------- Not ------- */
    } else if (tsdbQueryFilter instanceof NotFilter) {
      return buildFromTsdbFilter(
          ((NotFilter) tsdbQueryFilter).getFilter(),
          Filter.Operator.NOT);
    /** ------- Tag Value Filters ------- */
    } else if (tsdbQueryFilter instanceof TagValueFilter) {
      hasTagOrAnyFilter = true;
      TagValueFilter filter = (TagValueFilter) tsdbQueryFilter;
      if (filter instanceof TagValueLiteralOrFilter) {
        TagValueLiteralOrFilter tagVFilter = (TagValueLiteralOrFilter) filter;
        String[] values = tagVFilter.literals().stream().toArray(String[]::new);
        Set<String> extant = tags.get(filter.getTagKey());
        if (extant == null) {
          if (operator != Operator.NOT) {
            extant = Sets.newHashSet(tagVFilter.literals());
            tags.put(filter.getTagKey(), extant);
          }
        } else if (extant != SCAN_TAG_VALUES) {
          if (operator != Operator.NOT) {
            extant.addAll(tagVFilter.literals());
          }
        }
        return LiteralFilter.newBuilder()
            .forTag(tagVFilter.getTagKey())
            .withValues(values)
            .withOperator(operator)
            .build();
      } else if (filter instanceof TagValueRegexFilter) {
        final String regexp = filter.getFilter();
        tags.put(filter.getTagKey(), SCAN_TAG_VALUES);
        return RegexpFilter.newBuilder()
            .forTag(filter.getTagKey())
            .withValues(regexp)
            .withOperator(operator)
            .build();
      } else if (filter instanceof TagValueWildcardFilter) {
        final String filterString = filter.getFilter();
        String pattern = filterString.toLowerCase().replace("*", ".*");
        tags.put(filter.getTagKey(), SCAN_TAG_VALUES);
        return RegexpFilter.newBuilder()
            .forTag(filter.getTagKey())
            .withValues(pattern)
            .withOperator(operator)
            .build();
      } else {
        throw new UnsupportedOperationException("Unsupported OpenTSDB Filter " + filter.getClass());
      }
    /** ------- Tag Key Literal ------- */
    } else if (tsdbQueryFilter instanceof TagKeyLiteralOrFilter) {
      hasTagOrAnyFilter = true;
      String[] values = ((TagKeyLiteralOrFilter) tsdbQueryFilter).literals().stream().toArray(String[]::new);
      for (int i = 0; i < values.length; i++) {
        if (operator == Operator.NOT) {
          tags.remove(values[i]);
        } else {
          tags.put(values[i], SCAN_TAG_VALUES);
        }
      }
      return TagKeyFilter.newBuilder()
          .withValues(values)
          .withOperator(operator)
          .ofType(Type.LITERAL)
          .build();
    /** ------- Tag Key Regex ------- */
    } else if (tsdbQueryFilter instanceof TagKeyRegexFilter) {
      hasTagOrAnyFilter = true;
      scanTagKeys = true;
      String regexp = ((TagKeyRegexFilter) tsdbQueryFilter).filter();
      return TagKeyFilter.newBuilder()
          .withValues(regexp)
          .withOperator(operator)
          .ofType(Type.REGEXP)
          .build();
    /** ------- Metric Literal ------- */
    } else if (tsdbQueryFilter instanceof MetricLiteralFilter) {
      hasMetricFilter = true;
      return MetricFilter.newBuilder()
          .withValues(((MetricLiteralFilter) tsdbQueryFilter).getMetric())
          .withOperator(operator)
          .ofType(Type.LITERAL)
          .build();
    /** ------- Metric Regex ------- */
    } else if (tsdbQueryFilter instanceof MetricRegexFilter) {
      hasMetricFilter = true;
      scanMetrics = true;
      return MetricFilter.newBuilder()
          .withValues(((MetricRegexFilter) tsdbQueryFilter).getMetric())
          .withOperator(operator)
          .ofType(Type.REGEXP)
          .build();
    /** ------- Any ------- */
    } else if (tsdbQueryFilter instanceof AnyFieldRegexFilter) {
      hasTagOrAnyFilter = true;
      scanTagKeys = true;
      scanMetrics = true;
      return AnyFilter.newBuilder()
          .withValues(((AnyFieldRegexFilter) tsdbQueryFilter).getFilter())
          .withOperator(operator)
          .ofType(Type.REGEXP)
          .build();
    } else {
      // TODO - we also have Field literal and regex but those are for status' and
      // events.
      throw new UnsupportedOperationException(
          "Unsupported QueryFilter " + tsdbQueryFilter.getClass());
    }
  }

  /**
   * Walks the nested filter and sets the flags we need.
   * @param filter
   */
  void walkNestedFilter(final Filter filter) {
    if (filter instanceof net.opentsdb.aura.metrics.meta.ChainFilter) {
      if (filter.getOperator() == Operator.AND) {
        ((net.opentsdb.aura.metrics.meta.ChainFilter) filter).getChain().sort(FILTER_COMPARATOR);
        for (final Filter f : ((net.opentsdb.aura.metrics.meta.ChainFilter) filter).getChain()) {
          if (!(f instanceof net.opentsdb.aura.metrics.meta.ChainFilter) &&
              f.getOperator() != Operator.NOT) {
            // particularly needed for metrics.
            f.operator = Operator.AND;
          }
        }
      }
      // recurse.
      for (final Filter child : ((net.opentsdb.aura.metrics.meta.ChainFilter) filter).getChain()) {
        walkNestedFilter(child);
      }
      
      // yank any's
      Iterator<Filter> iterator = ((net.opentsdb.aura.metrics.meta.ChainFilter) filter).getChain().iterator();
      while (iterator.hasNext()) {
        Filter f = iterator.next();
        if (f instanceof AnyFilter) {
          iterator.remove();
        }
      }
      // TODO - what to do if the chan is now empty?
    } else if (filter instanceof MetricFilter) {
      hasMetricFilter = true;
      if (filter.getType() == Type.REGEXP) {
        scanMetrics = true;
      } else {
        if (filter.getOperator() == Operator.NOT) {
          for (int i = 0; i < filter.getTagValues().length; i++) {
            metrics.remove(filter.getTagValues()[i]);
          }
        } else {
          for (int i = 0; i < filter.getTagValues().length; i++) {
            metrics.add(filter.getTagValues()[i]);
          }
        }
      }
    } else if (filter instanceof AnyFilter) {
      hasTagOrAnyFilter = true;
      scanMetrics = true;
      scanTagKeys = true;
      
      if (filter.getOperator() == Operator.NOT) {
        if (notAnyFilterBuilder == null) {
          notAnyFilterBuilder = AnyFilter.newBuilder()
              .withValues(filter.getTagValues()[0])
              .withOperator(filter.getOperator());
        } else {
          notAnyFilterBuilder.addValue(filter.getTagValues()[0]);
        }
      } else {
        if (anyFilterBuilder == null) {
          anyFilterBuilder = AnyFilter.newBuilder()
              .withValues(filter.getTagValues()[0])
              .withOperator(filter.getOperator());
        } else {
          anyFilterBuilder.addValue(filter.getTagValues()[0]);
        }
      }
      
    } else if (filter instanceof TagKeyFilter) {
      hasTagOrAnyFilter = true;
      if (filter.getType() == Type.LITERAL) {
        for (int i = 0; i < filter.getTagValues().length; i++) {
          tags.put(filter.getTagValues()[i], SCAN_TAG_VALUES);
        }
      } else {
        scanTagKeys = true;
      }
    } else if (filter instanceof LiteralFilter) {
      hasTagOrAnyFilter = true;
      if (!Strings.isNullOrEmpty(filter.getTagKey())) {
        Set<String> extant = tags.get(filter.getTagKey());
        if (filter.getOperator() == Operator.NOT) {
          if (extant != null && extant != SCAN_TAG_VALUES) {
            for (int i = 0; i < filter.getTagValues().length; i++) {
              extant.remove(filter.getTagValues()[i]);
            }
          }
        } else {
          if (extant == null) {
            tags.put(filter.getTagKey(), Sets.newHashSet(filter.getTagValues()));
          } else if (extant != SCAN_TAG_VALUES) {
            for (int i = 0; i < filter.getTagValues().length; i++) {
              extant.add(filter.getTagValues()[i]);
            }
          }
        }
      }
    } else if (filter instanceof RegexpFilter) {
      hasTagOrAnyFilter = true;
      if (!Strings.isNullOrEmpty(filter.getTagKey())) {
        if (filter.getOperator() == Operator.NOT) {
          if (filter.matchAll()) {
            // special case wherein we don't want anything for this tag key.
            tags.remove(filter.getTagKey());
          } else {
            tags.put(filter.getTagKey(), SCAN_TAG_VALUES);
          }
        } else {
          tags.put(filter.getTagKey(), SCAN_TAG_VALUES);
        }
      }
    }
  }
  
  static class FilterComparator implements Comparator<Filter> {

    @Override
    public int compare(final Filter o1, final Filter o2) {
      if (o1 == null || o2 == null) {
        throw new NullPointerException("One or the other filter was null.");
      }
      if (o1 == o2) {
        return 0;
      }
      
      if (o1.getOperator() == Operator.NOT && 
          o2.getOperator() != Operator.NOT) {
        return 1;
      } else if (o1.getOperator() != Operator.NOT &&
                 o2.getOperator() == Operator.NOT) {
        return -1;
      }
      
      if (o1 instanceof MetricFilter && !(o2 instanceof MetricFilter)) {
        return -1;
      }
      
      return 0;
    }
    
  }
  static final FilterComparator FILTER_COMPARATOR = new FilterComparator();
}