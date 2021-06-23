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


public class RegexpFilter extends Filter {
  private RegexpFilter(String tagKey, String[] tagValues, Operator operator) {
    super(tagKey, tagValues, operator, Type.REGEXP);
  }

  @Override
  public boolean match(final StringType type, final String value) {
    if (type != StringType.TAG_VALUE) {
      return operator == Operator.NOT ? true : false;
    }
    final boolean matched = pattern.matcher(value).find();
    if (operator == Operator.NOT) {
      return !matched;
    }
    return matched;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends Filter.Builder<Builder, RegexpFilter> {
    @Override
    public RegexpFilter build() {
      return new RegexpFilter(tag, values, operator);
    }
  }
}