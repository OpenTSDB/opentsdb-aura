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

package net.opentsdb.aura.metrics.meta;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class MetaQueryBuilder extends QueryBuilder {

  public static final MetaQuery META_MATCH_ALL_QUERY =
      new MatchAllQuery.MetaMatchAllQuery(newBuilder());

  public static enum MetaQueryType {
    NAMESPACES,
    METRICS,
    TAG_KEYS,
    TAG_VALUES,
    TAG_KEYS_AND_VALUES,
    TIMESERIES,
    BASIC
  }

  // equality marker to tell us to scan meta.
  public static final Set<String> SCAN_TAG_VALUES =
      new HashSet() {
        {
          add("SCAN_TAG_VALUES");
        }
      };

  protected Set<String> metrics;
  protected AnyFilter.Builder anyFilterBuilder;
  protected AnyFilter.Builder notAnyFilterBuilder;
  protected boolean hasMetricFilter;
  protected boolean hasTagOrAnyFilter;
  protected boolean scanMetrics;
  protected boolean scanTagKeys;
  protected MetaQueryType metaQueryType;
  protected String metaAggregationField;

  protected MetaQueryBuilder() {
    super();
    metrics = new HashSet<>();
  }

  public static MetaQueryBuilder newBuilder() {
    return new MetaQueryBuilder();
  }

  public static Query matchAllQuery() {
    return MATCH_ALL_QUERY;
  }

  //  public MetaQueryBuilder fromTSDBQueryFilter(QueryFilter tsdbQueryFilter) {
  //    if (tsdbQueryFilter != null) {
  //      nestedFilter = buildFromTsdbFilter(tsdbQueryFilter, null);
  //    }
  //    return this;
  //  }

  public MetaQueryBuilder fromNestedFilter(Filter nestedFilter) {
    this.nestedFilter = nestedFilter;
    return this;
  }

  public MetaQueryBuilder exactMatch(boolean exactMatch) {
    this.exactMatch = exactMatch;
    return this;
  }

  public MetaQueryBuilder setTagCount(int tagCount) {
    this.tagCount = tagCount;
    return this;
  }

  public MetaQueryBuilder setMetaQueryType(MetaQueryType metaQueryType) {
    this.metaQueryType = metaQueryType;
    return this;
  }

  public MetaQueryBuilder setMetaAggregationField(String metaAggregationField) {
    this.metaAggregationField = metaAggregationField;
    return this;
  }

  public MetaQuery build() {
    if (null == nestedFilter) {
      return META_MATCH_ALL_QUERY;
    }

    if (metaQueryType != null) {
      hasMetricFilter = false;
      tagCount = 0;
      walkNestedFilter(nestedFilter);
      return new MetaQuery(this);
    }

    boolean notFilter = nestedFilter.isNotFilter();
    if (exactMatch && notFilter) {
      throw new IllegalArgumentException("Negative filtering not allowed for exact match");
    }

    if (notFilter) {
      nestedFilter = ChainFilter.newBuilder(Filter.MATCH_ALL_FILTER).not(nestedFilter).build();
    }
    return new MetaQuery(this);
  }

  //  Filter buildFromTsdbFilter(
  //      final QueryFilter tsdbQueryFilter,
  //      final Operator operator) {
  //    /** ------- Explicit tags ------- */
  //    if (tsdbQueryFilter instanceof ExplicitTagsFilter) {
  //      return buildFromTsdbFilter(
  //          ((ExplicitTagsFilter) tsdbQueryFilter).getFilter(), null);
  //    /** ------- Chain ------- */
  //    } else if (tsdbQueryFilter instanceof ChainFilter) {
  //      ChainFilter chainFilter = (ChainFilter) tsdbQueryFilter;
  //      List<QueryFilter> queryFilters = chainFilter.getFilters();
  //      ChainFilter.FilterOp op = chainFilter.getOp();
  //
  //      Operator chainOperator =
  //          Operator.valueOf(Operator.class, op.toString());
  //      List<Filter> chain =
  //          queryFilters.stream()
  //              .map(filter -> buildFromTsdbFilter(filter, chainOperator))
  //              .collect(Collectors.toList());
  //
  //      // Nots are a "fun" case. If we have a chain like:
  //      // "NOT .*foo AND a.*" then we ACTUALLY want the NOT after the positive
  //      // matches so we can sort the list. OR's it doesn't matter, we'd get every-
  //      // thing in that case.
  //      if (op == ChainFilter.FilterOp.AND) {
  //        chain.sort(FILTER_COMPARATOR);
  //      }
  //      return ChainFilter.newBuilder()
  //          .addChain(chain)
  //          .withOperator(operator)
  //          .build();
  //    /** ------- Not ------- */
  //    } else if (tsdbQueryFilter instanceof NotFilter) {
  //      return buildFromTsdbFilter(
  //          ((NotFilter) tsdbQueryFilter).getFilter(),
  //          Operator.NOT);
  //    /** ------- Tag Value Filters ------- */
  //    } else if (tsdbQueryFilter instanceof TagValueFilter) {
  //      hasTagOrAnyFilter = true;
  //      TagValueFilter filter = (TagValueFilter) tsdbQueryFilter;
  //      if (filter instanceof TagValueLiteralOrFilter) {
  //        TagValueLiteralOrFilter tagVFilter = (TagValueLiteralOrFilter) filter;
  //        String[] values = tagVFilter.literals().stream().toArray(String[]::new);
  //        Set<String> extant = tags.get(filter.getTagKey());
  //        if (extant == null) {
  //          if (operator != Operator.NOT) {
  //            extant = Sets.newHashSet(tagVFilter.literals());
  //            tags.put(filter.getTagKey(), extant);
  //          }
  //        } else if (extant != SCAN_TAG_VALUES) {
  //          if (operator != Operator.NOT) {
  //            extant.addAll(tagVFilter.literals());
  //          }
  //        }
  //        return LiteralFilter.newBuilder()
  //            .forTag(tagVFilter.getTagKey())
  //            .withValues(values)
  //            .withOperator(operator)
  //            .build();
  //      } else if (filter instanceof TagValueRegexFilter) {
  //        final String regexp = filter.getFilter();
  //        tags.put(filter.getTagKey(), SCAN_TAG_VALUES);
  //        return RegexpFilter.newBuilder()
  //            .forTag(filter.getTagKey())
  //            .withValues(regexp)
  //            .withOperator(operator)
  //            .build();
  //      } else if (filter instanceof TagValueWildcardFilter) {
  //        final String filterString = filter.getFilter();
  //        String pattern = filterString.toLowerCase().replace("*", ".*");
  //        tags.put(filter.getTagKey(), SCAN_TAG_VALUES);
  //        return RegexpFilter.newBuilder()
  //            .forTag(filter.getTagKey())
  //            .withValues(pattern)
  //            .withOperator(operator)
  //            .build();
  //      } else {
  //        throw new UnsupportedOperationException("Unsupported OpenTSDB Filter " +
  // filter.getClass());
  //      }
  //    /** ------- Tag Key Literal ------- */
  //    } else if (tsdbQueryFilter instanceof TagKeyLiteralOrFilter) {
  //      hasTagOrAnyFilter = true;
  //      String[] values = ((TagKeyLiteralOrFilter)
  // tsdbQueryFilter).literals().stream().toArray(String[]::new);
  //      for (int i = 0; i < values.length; i++) {
  //        if (operator == Operator.NOT) {
  //          tags.remove(values[i]);
  //        } else {
  //          tags.put(values[i], SCAN_TAG_VALUES);
  //        }
  //      }
  //      return TagKeyFilter.newBuilder()
  //          .withValues(values)
  //          .withOperator(operator)
  //          .ofType(Type.LITERAL)
  //          .build();
  //    /** ------- Tag Key Regex ------- */
  //    } else if (tsdbQueryFilter instanceof TagKeyRegexFilter) {
  //      hasTagOrAnyFilter = true;
  //      scanTagKeys = true;
  //      String regexp = ((TagKeyRegexFilter) tsdbQueryFilter).filter();
  //      return TagKeyFilter.newBuilder()
  //          .withValues(regexp)
  //          .withOperator(operator)
  //          .ofType(Type.REGEXP)
  //          .build();
  //    /** ------- Metric Literal ------- */
  //    } else if (tsdbQueryFilter instanceof MetricLiteralFilter) {
  //      hasMetricFilter = true;
  //      return MetricFilter.newBuilder()
  //          .withValues(((MetricLiteralFilter) tsdbQueryFilter).getMetric())
  //          .withOperator(operator)
  //          .ofType(Type.LITERAL)
  //          .build();
  //    /** ------- Metric Regex ------- */
  //    } else if (tsdbQueryFilter instanceof MetricRegexFilter) {
  //      hasMetricFilter = true;
  //      scanMetrics = true;
  //      return MetricFilter.newBuilder()
  //          .withValues(((MetricRegexFilter) tsdbQueryFilter).getMetric())
  //          .withOperator(operator)
  //          .ofType(Type.REGEXP)
  //          .build();
  //    /** ------- Any ------- */
  //    } else if (tsdbQueryFilter instanceof AnyFieldRegexFilter) {
  //      hasTagOrAnyFilter = true;
  //      scanTagKeys = true;
  //      scanMetrics = true;
  //      return AnyFilter.newBuilder()
  //          .withValues(((AnyFieldRegexFilter) tsdbQueryFilter).getFilter())
  //          .withOperator(operator)
  //          .ofType(Type.REGEXP)
  //          .build();
  //    } else {
  //      // TODO - we also have Field literal and regex but those are for status' and
  //      // events.
  //      throw new UnsupportedOperationException(
  //          "Unsupported QueryFilter " + tsdbQueryFilter.getClass());
  //    }
  //  }

  /**
   * Walks the nested filter and sets the flags we need.
   *
   * @param filter
   */
  void walkNestedFilter(final Filter filter) {
    if (filter instanceof ChainFilter) {
      if (filter.getOperator() == Filter.Operator.AND) {
        ((ChainFilter) filter).getChain().sort(FILTER_COMPARATOR);
        for (final Filter f : ((ChainFilter) filter).getChain()) {
          if (!(f instanceof ChainFilter) && f.getOperator() != Filter.Operator.NOT) {
            // particularly needed for metrics.
            f.operator = Filter.Operator.AND;
          }
        }
      }
      // recurse.
      for (final Filter child : ((ChainFilter) filter).getChain()) {
        walkNestedFilter(child);
      }

      // yank any's
      Iterator<Filter> iterator = ((ChainFilter) filter).getChain().iterator();
      while (iterator.hasNext()) {
        Filter f = iterator.next();
        if (f instanceof AnyFilter) {
          iterator.remove();
        }
      }
      // TODO - what to do if the chan is now empty?
    } else if (filter instanceof MetricFilter) {
      hasMetricFilter = true;
      if (filter.getType() == Filter.Type.REGEXP) {
        scanMetrics = true;
      } else {
        if (filter.getOperator() == Filter.Operator.NOT) {
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

      if (filter.getOperator() == Filter.Operator.NOT) {
        if (notAnyFilterBuilder == null) {
          notAnyFilterBuilder =
              AnyFilter.newBuilder()
                  .withValues(filter.getTagValues()[0])
                  .withOperator(filter.getOperator());
        } else {
          notAnyFilterBuilder.addValue(filter.getTagValues()[0]);
        }
      } else {
        if (anyFilterBuilder == null) {
          anyFilterBuilder =
              AnyFilter.newBuilder()
                  .withValues(filter.getTagValues()[0])
                  .withOperator(filter.getOperator());
        } else {
          anyFilterBuilder.addValue(filter.getTagValues()[0]);
        }
      }

    } else if (filter instanceof TagKeyFilter) {
      hasTagOrAnyFilter = true;
      if (filter.getType() == Filter.Type.LITERAL) {
        for (int i = 0; i < filter.getTagValues().length; i++) {
          tags.put(filter.getTagValues()[i], SCAN_TAG_VALUES);
        }
      } else {
        scanTagKeys = true;
      }
    } else if (filter instanceof LiteralFilter) {
      hasTagOrAnyFilter = true;
      String tagKey = filter.getTagKey();
      if (tagKey != null && !tagKey.isEmpty()) {
        Set<String> extant = tags.get(tagKey);
        if (filter.getOperator() == Filter.Operator.NOT) {
          if (extant != null && extant != SCAN_TAG_VALUES) {
            for (int i = 0; i < filter.getTagValues().length; i++) {
              extant.remove(filter.getTagValues()[i]);
            }
          }
        } else {
          if (extant == null) {
            Set<String> newHashSet = new HashSet<>();
            Collections.addAll(newHashSet, filter.getTagValues());
            tags.put(tagKey, newHashSet);
          } else if (extant != SCAN_TAG_VALUES) {
            for (int i = 0; i < filter.getTagValues().length; i++) {
              extant.add(filter.getTagValues()[i]);
            }
          }
        }
      }
    } else if (filter instanceof RegexpFilter) {
      hasTagOrAnyFilter = true;
      String tagKey = filter.getTagKey();
      if (tagKey != null && !tagKey.isEmpty()) {
        if (filter.getOperator() == Filter.Operator.NOT) {
          if (filter.matchAll()) {
            // special case wherein we don't want anything for this tag key.
            tags.remove(tagKey);
          } else {
            tags.put(tagKey, SCAN_TAG_VALUES);
          }
        } else {
          tags.put(tagKey, SCAN_TAG_VALUES);
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

      if (o1.getOperator() == Filter.Operator.NOT && o2.getOperator() != Filter.Operator.NOT) {
        return 1;
      } else if (o1.getOperator() != Filter.Operator.NOT && o2.getOperator() == Filter.Operator.NOT) {
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
