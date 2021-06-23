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

import org.roaringbitmap.RoaringBitmap;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TagKeyFilter extends Filter {

  Set<String> keys;
  // the values are the key literals or pattern
  protected TagKeyFilter(String tagKey, String[] tagValues, Operator operator, Type type) {
    super(tagKey, tagValues, operator, type);
    if (type == Type.LITERAL) {
      keys = new HashSet<>(tagValues.length);
      Collections.addAll(keys, tagValues);
    }
  }

  @Override
  public RoaringBitmap apply(final Map<String, Map<String, RoaringBitmap>> indexMap) {
    return null;
  }

  @Override
  public boolean match(final StringType type, final String value) {
    if (type != StringType.TAG_KEY) {
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
      final boolean matchedKey = keys.contains(value);
      if (operator == Operator.NOT) {
        return !matchedKey;
      }
      return matchedKey;
    default:
      throw new UnsupportedOperationException("Unknown type: " + this.type);
    }
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends Filter.Builder<Builder, TagKeyFilter> {
    @Override
    public TagKeyFilter build() {
      return new TagKeyFilter(tag, values, operator, type);
    }
  }

}