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


import net.opentsdb.aura.metrics.meta.Filter.Operator;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class QueryBuilder {

  public static final Query MATCH_ALL_QUERY = new MatchAllQuery(newBuilder());

  protected Filter nestedFilter;
  protected boolean exactMatch;
  protected Map<String, Set<String>> tags;
  protected int tagCount;

  protected QueryBuilder() {
    tags = new HashMap<>();
  }

  public static QueryBuilder newBuilder() {
    return new QueryBuilder();
  }

  public static Query matchAllQuery() {
    return MATCH_ALL_QUERY;
  }

  public QueryBuilder fromNestedFilter(Filter nestedFilter) {
    this.nestedFilter = nestedFilter;
    return this;
  }

  public QueryBuilder exactMatch(boolean exactMatch) {
    this.exactMatch = exactMatch;
    return this;
  }

  public QueryBuilder setTagCount(int tagCount) {
    this.tagCount = tagCount;
    return this;
  }

  public Query build() {
    if (null == nestedFilter) {
      return MATCH_ALL_QUERY;
    }
    
    boolean notFilter = nestedFilter.isNotFilter();
    if (exactMatch && notFilter) {
      throw new IllegalArgumentException("Negative filtering not allowed for exact match");
    }
  
    if (notFilter) {
      nestedFilter = ChainFilter
          .newBuilder(Filter.MATCH_ALL_FILTER)
          .not(nestedFilter)
          .build();
    }
    return new Query(this);
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