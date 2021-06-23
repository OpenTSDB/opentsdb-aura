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

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.regex.Pattern;

public class MetricFilter extends Filter {

  Pattern regex;
  Set<String> metrics;
  
  // No tag key of course.
  protected MetricFilter(String tagKey, String[] tagValues, Operator operator, Type type) {
    super(NewDocStore.METRICS, tagValues, operator, type);
    switch (type) {
    case REGEXP:
      regex = Pattern.compile(tagValues[0]);
      break;
    case LITERAL:
      metrics = Sets.newHashSet(tagValues);
    default:
      // no-op
    }
  }
  
  @Override
  public boolean match(final StringType type, final String value) {
    if (type != StringType.METRIC) {
      return operator == Operator.NOT ? true : false;
    }
    
    switch (this.type) {
    case REGEXP:
      final boolean matched = pattern.matcher(value).find();
      if (operator == Operator.NOT) {
        return !matched;
      }
      return matched;
    case LITERAL:
      final boolean matchedMetric = metrics.contains(value);
      if (operator == Operator.NOT) {
        return !matchedMetric;
      }
      return matchedMetric;
    default:
      throw new UnsupportedOperationException("Unknown type: " + this.type);
    }
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends Filter.Builder<Builder, MetricFilter> {
    @Override
    public MetricFilter build() {
      return new MetricFilter(tag, values, operator, type);
    }
  }
  
}