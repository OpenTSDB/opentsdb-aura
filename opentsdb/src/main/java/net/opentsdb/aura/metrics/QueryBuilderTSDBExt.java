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

import net.opentsdb.aura.metrics.meta.Filter;
import net.opentsdb.aura.metrics.meta.LiteralFilter;
import net.opentsdb.aura.metrics.meta.MetricFilter;
import net.opentsdb.aura.metrics.meta.QueryBuilder;
import net.opentsdb.aura.metrics.meta.RegexpFilter;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import net.opentsdb.aura.metrics.meta.Filter.Operator;

public class QueryBuilderTSDBExt extends QueryBuilder {

  public static QueryBuilderTSDBExt newBuilder() {
    return new QueryBuilderTSDBExt();
  }

  public QueryBuilderTSDBExt fromTSDBQueryFilter(QueryFilter tsdbQueryFilter) {
    if (tsdbQueryFilter != null) {
      Set<String> uniqueTagKeys = Sets.newHashSet();
      nestedFilter = buildFromTsdbFilter(tsdbQueryFilter, uniqueTagKeys,null);
      if (tsdbQueryFilter instanceof ExplicitTagsFilter) {
        exactMatch(true);
      }
      tagCount = uniqueTagKeys.size();
    }
    return this;
  }

  static Filter buildFromTsdbFilter(
          final QueryFilter tsdbQueryFilter,
          final Set<String> uniqTagKeys,
          Filter.Operator operator) {
    if (tsdbQueryFilter instanceof ExplicitTagsFilter) {
      return buildFromTsdbFilter(
              ((ExplicitTagsFilter) tsdbQueryFilter).getFilter(), uniqTagKeys, null);
    } else if (tsdbQueryFilter instanceof ChainFilter) {
      ChainFilter chainFilter = (ChainFilter) tsdbQueryFilter;
      List<QueryFilter> queryFilters = chainFilter.getFilters();
      ChainFilter.FilterOp op = chainFilter.getOp();

      Filter.Operator chainOperator =
              Filter.Operator.valueOf(op.name());
      List<Filter> chain =
              queryFilters.stream()
                      .map(filter -> buildFromTsdbFilter(filter, uniqTagKeys, chainOperator))
                      .collect(Collectors.toList());
      return net.opentsdb.aura.metrics.meta.ChainFilter.newBuilder()
              .addChain(chain)
              .withOperator(operator)
              .build();
    } else if (tsdbQueryFilter instanceof NotFilter) {
      return buildFromTsdbFilter(
              ((NotFilter) tsdbQueryFilter).getFilter(),
              uniqTagKeys,
              Filter.Operator.NOT);
    } else if (tsdbQueryFilter instanceof TagValueFilter) {
      TagValueFilter filter = (TagValueFilter) tsdbQueryFilter;
      uniqTagKeys.add(filter.getTagKey());
      if (filter instanceof TagValueLiteralOrFilter) {
        TagValueLiteralOrFilter tagVFilter = (TagValueLiteralOrFilter) filter;
        String[] values = tagVFilter.literals().stream().toArray(String[]::new);
        return LiteralFilter.newBuilder()
                .forTag(tagVFilter.getTagKey())
                .withValues(values)
                .withOperator(operator)
                .build();
      } else if (filter instanceof TagValueRegexFilter) {
        final String regexp = filter.getFilter();
        return RegexpFilter.newBuilder()
                .forTag(filter.getTagKey())
                .withValues(regexp)
                .withOperator(operator)
                .build();
      } else if (filter instanceof TagValueWildcardFilter) {
        final String filterString = filter.getFilter();
        String pattern = filterString.replace("*", ".*");
        return RegexpFilter.newBuilder()
                .forTag(filter.getTagKey())
                .withValues(pattern)
                .withOperator(operator)
                .build();
      } else {
        throw new UnsupportedOperationException("Unsupported TagValueFilter " + filter.getClass());
      }
    } else {
      throw new UnsupportedOperationException(
              "Unsupported QueryFilter " + tsdbQueryFilter.getClass());
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