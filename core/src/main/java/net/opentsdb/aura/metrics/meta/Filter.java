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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Filter {

  public static final Filter MATCH_ALL_FILTER = matchAllFilter();

  public static enum StringType {
    METRIC,
    TAG_KEY,
    TAG_VALUE
  }

  public Operator operator;

  protected Type type;
  public String tagKey;
  protected String[] tagValues;
  protected Pattern pattern;
  protected boolean matches_all;

  protected Filter(String tagKey, String[] tagValues, Operator operator, Type type) {
    this.type = type;
    this.tagKey = tagKey;
    this.tagValues = tagValues;
    this.operator = operator == null ? Operator.OR : operator;
    if (type == Type.REGEXP) {
      pattern = Pattern.compile(tagValues[0]);
      if (tagValues[0].trim().equals(".*") ||
          tagValues[0].trim().equals("^.*") ||
          tagValues[0].trim().equals(".*$") ||
          tagValues[0].trim().equals("^.*$")) {
        // yeah there are many more permutations but these are the most likely
        // to be encountered in the wild.
        matches_all = true;
      } else {
        matches_all = false;
      }
    }
  }

  public Operator getOperator() {
    return operator;
  }

  public Type getType() {
    return type;
  }

  public String getTagKey() {
    return tagKey;
  }

  public String[] getTagValues() {
    return tagValues;
  }

  public static Filter matchAllFilter() {
    return new MatchAllFilter();
  }

  @JsonIgnore
  public boolean isNotFilter() {
    return operator != null && operator == Operator.NOT;
  }

  public boolean matchAll() {
    return matches_all;
  }

  public boolean matchesPattern(final String string) {
    if (pattern == null) {
      return false;
    }
    return pattern.matcher(string).find();
  }

  public RoaringBitmap apply(final Map<String, Map<String, RoaringBitmap>> indexMap) {
    Map<String, RoaringBitmap> valueMap = indexMap.get(tagKey);
    RoaringBitmap rr = new RoaringBitmap(); // empty bitmap
    if (null != valueMap) {
      type.apply(rr, valueMap, tagValues);
    }
    return rr;
  }

  public void apply(final RoaringBitmap filterRR, final Map<String, RoaringBitmap> valueMap) {
    type.apply(filterRR, valueMap, tagValues);
  }

  public abstract boolean match(final StringType type, final String value);

  @Override
  public String toString() {
    return new StringBuilder()
        .append("{class=")
        .append(getClass().getSimpleName())
        .append(", tagKey=")
        .append(tagKey)
        .append(", operator=")
        .append(operator)
        .append(", type=")
        .append(type)
        .append(", tagValues=")
        .append(tagValues == null ? null : Arrays.toString(tagValues))
        .append("}")
        .toString();
  }

  public enum Operator {
    AND {
      @Override
      public void aggregate(RoaringBitmap bitmap1, RoaringBitmap bitmap2) {
        bitmap1.and(bitmap2);
      }
    },
    OR {
      @Override
      public void aggregate(RoaringBitmap bitmap1, RoaringBitmap bitmap2) {
        bitmap1.or(bitmap2);
      }
    },
    NOT {
      @Override
      public void aggregate(RoaringBitmap bitmap1, RoaringBitmap bitmap2) {
        bitmap1.andNot(bitmap2);
      }
    };

    public abstract void aggregate(RoaringBitmap bitmap1, RoaringBitmap bitmap2);
  }

  public enum Type {
    REGEXP {
      @Override
      public void apply(
          RoaringBitmap filterRR, Map<String, RoaringBitmap> valueMap, String[] tagValues) {
        Pattern pattern = Pattern.compile(tagValues[0].trim());
        for (String value : valueMap.keySet()) {
          boolean matched = pattern.matcher(value).find();
          if (matched) {
            RoaringBitmap valueRR = valueMap.get(value);
            filterRR.or(valueRR);
          }
        }
      }
    },
    LITERAL {
      @Override
      public void apply(
          RoaringBitmap filterRR, Map<String, RoaringBitmap> valueMap, String[] tagValues) {
        for (int i = 0; i < tagValues.length; i++) {
          RoaringBitmap valueRR = valueMap.get(tagValues[i]);
          if (null != valueRR) {
            filterRR.or(valueRR);
          }
        }
      }
    };

    public abstract void apply(
        RoaringBitmap filterRR, Map<String, RoaringBitmap> valueMap, String[] tagValues);
  }

  public abstract static class Builder<B extends Builder, F extends Filter> {
    protected Operator operator;

    protected String tag;
    protected String[] values;
    protected Type type;

    public B and() {
      return withOperator(Operator.AND);
    }

    public B or() {
      return withOperator(Operator.OR);
    }

    public B not() {
      return withOperator(Operator.NOT);
    }

    public B withOperator(Operator operator) {
      this.operator = operator;
      return (B) this;
    }

    public B forTag(String tag) {
      this.tag = tag;
      return (B) this;
    }

    public B withValues(String... values) {
      this.values = values;
      return (B) this;
    }

    public B addValue(String value) {
      String[] temp = new String[values.length + 1];
      System.arraycopy(values, 0, temp, 0, values.length);
      values = temp;
      values[values.length - 1] = value;
      return (B) this;
    }

    public B ofType(Type type) {
      this.type = type;
      return (B) this;
    }

    public abstract F build();
  }
}
