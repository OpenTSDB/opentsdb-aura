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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ChainFilter extends Filter {
  private List<Filter> chain;

  @JsonIgnore private boolean chainAllOr;
  @JsonIgnore private boolean chainAllAnd;
  @JsonIgnore private boolean chainAllNot;

  private ChainFilter(
      final List<Filter> chain,
      final Operator operator,
      final boolean chainAllOr,
      final boolean chainAllAnd,
      final boolean chainAllNot) {
    super(null, null, operator, null);
    this.chain = chain;
    this.chainAllOr = chainAllOr;
    this.chainAllAnd = chainAllAnd;
    this.chainAllNot = chainAllNot;
  }

  private ChainFilter(final Filter filter, final Operator operator) {
    super(null, null, operator, null);
    chain = new ArrayList<>();
    chain.add(filter);
  }

  public List<Filter> getChain() {
    return chain;
  }

  @Override
  public boolean isNotFilter() {
    return super.isNotFilter() || chainAllNot;
  }

  @Override
  public String getTagKey() {
    return null;
  }

  @Override
  public String[] getTagValues() {
    return null;
  }

  @Override
  public Type getType() {
    return null;
  }

  @Override
  public RoaringBitmap apply(final Map<String, Map<String, RoaringBitmap>> indexMap) {
    RoaringBitmap rr = null;
    RoaringBitmap negativeBitmap = null;
    for (int i = 0; i < chain.size(); i++) {
      Filter filter = chain.get(i);
      Operator operator = filter.getOperator();
      RoaringBitmap bitmap = filter.apply(indexMap);

      if (filter.isNotFilter()) {
        if (negativeBitmap == null) {
          negativeBitmap = bitmap;
        } else {
          negativeBitmap.or(bitmap);
        }
      } else {
        if (rr == null) {
          // First positive filter in the chain
          rr = bitmap;
        } else {
          operator.aggregate(rr, bitmap);
        }
      }
    }
    if (negativeBitmap != null) {
      if (rr == null) {
        rr = negativeBitmap;
      } else {
        rr.andNot(negativeBitmap);
      }
    }
    return rr;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("{class=")
        .append(getClass().getSimpleName())
        .append(", operator=")
        .append(operator)
        .append(", type=")
        .append(type)
        .append(", filters=")
        .append(chain)
        .append(", chainCount=")
        .append(chain.size())
        .append("}")
        .toString();
  }
  
  @Override
  public boolean match(final StringType type, final String value) {
    if (operator == Operator.AND) {
      for (int i = 0; i < chain.size(); i++) {
        Filter child = chain.get(i);
        if (child instanceof MetricFilter && type != StringType.METRIC) {
          continue;
        }
        if (!child.match(type, value)) {
          return false;
        }
      }
      return true;
    } else {
      // no nots
      for (int i = 0; i < chain.size(); i++) {
        Filter child = chain.get(i);
        if (child instanceof MetricFilter && type != StringType.METRIC) {
          continue;
        }
        if (child.match(type, value)) {
          return true;
        }
      }
      return false;
    }
  }
  
  public boolean isChainAllOr() {
    return chainAllOr;
  }

  public boolean isChainAllAnd() {
    return chainAllAnd;
  }

  public boolean isChainAllNot() {
    return chainAllNot;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(final Filter filter) {
    return new Builder(filter);
  }

  public static class Builder extends Filter.Builder<Builder, ChainFilter> {

    private List<Filter> chain;

    private boolean chainAllOr;
    private boolean chainAllAnd;
    private boolean chainAllNot;

    private Builder() {
      this.chain = new ArrayList<>();
    }

    private Builder(Filter filter) {
      this();
      addToChain(filter);
    }

    public Builder and(Filter filter) {
      filter.operator = Operator.AND;
      return addToChain(filter);
    }

    public Builder or(Filter filter) {
      filter.operator = Operator.OR;
      return addToChain(filter);
    }

    public Builder not(Filter filter) {
      filter.operator = Operator.NOT;
      return addToChain(filter);
    }

    public Builder addChain(Filter... filters) {
      for (Filter filter : filters) {
        addToChain(filter);
      }
      return this;
    }
    
    public Builder addChain(Collection<Filter> filters) {
      for (Filter filter : filters) {
        addToChain(filter);
      }
      return this;
    }

    private Builder addToChain(Filter filter) {
      this.chain.add(filter);

      // if all are nots we switch to not
      int nots = 0;
      for (Filter f : chain) {
        if (f.getOperator() == Operator.NOT) {
          nots++;
        }
      }
      
      if (operator == null) {
        chainAllOr = true;
        chainAllAnd = true;
      } else {
        if (!chainAllOr && !chainAllAnd && !chainAllNot) {
          chainAllOr = operator == Operator.OR;
          chainAllAnd = operator == Operator.AND;
          chainAllNot = operator == Operator.NOT;
        } else {
          chainAllOr &= operator == Operator.OR;
          chainAllAnd &= operator == Operator.AND;
          chainAllNot &= operator == Operator.NOT;
        }
      }
      
      if (nots == chain.size()) {
        chainAllNot = true;
      } else {
        chainAllNot = false;
      }
      return this;
    }

    @Override
    public ChainFilter build() {
      if (chain == null || chain.isEmpty()) {
        throw new IllegalArgumentException("Chain is Empty");
      } else {
        if (chain.size() == 1) {
          Filter filter = chain.get(0);
          return new ChainFilter(filter, operator != null ? operator : filter.getOperator());
        }
        return new ChainFilter(chain, operator, chainAllOr, chainAllAnd, chainAllNot);
      }
    }
  }
}