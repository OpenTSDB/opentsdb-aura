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
import java.util.HashSet;
import java.util.Set;

public class LiteralFilter extends Filter {
  Set<String> values;
  private LiteralFilter(String tagKey, String[] tagValues, Operator operator) {
    super(tagKey, tagValues, operator, Type.LITERAL);
    values = new HashSet<>(tagValues.length);
    Collections.addAll(values, tagValues);
  }

  @Override
  public boolean match(final StringType type, final String value) {
    if (type != StringType.TAG_VALUE) {
      return operator == Operator.NOT ? true : false;
    }
    final boolean matched = values.contains(value);
    if (operator == Operator.NOT) {
      return !matched;
    }
    return matched;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends Filter.Builder<Builder, LiteralFilter> {
    @Override
    public LiteralFilter build() {
      return new LiteralFilter(tag, values, operator);
    }
  }
}