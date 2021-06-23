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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class AnyFilter extends Filter {

  // any is a "fun" case. We're going to merge them.
  List<Pattern> patterns;
  
  // No tag key of course.
  protected AnyFilter(String tagKey, String[] tagValues, Operator operator, Type type) {
    super(tagKey, tagValues, operator, type);
    patterns = new ArrayList<>();
    for (int i = 0; i < tagValues.length; i++) {
      patterns.add(Pattern.compile(tagValues[i]));
    }
  }

  @Override
  public RoaringBitmap apply(final Map<String, Map<String, RoaringBitmap>> indexMap) {
    // TODO - still don't know if this is right. We'd only look at the bitmaps at
    // this level if we are looking for tag values.
    RoaringBitmap rr = new RoaringBitmap(); // empty bitmap
    Map<String, RoaringBitmap> valueMap = indexMap.get("_metric");
    if (null != valueMap) {
      type.apply(rr, valueMap, tagValues);
    }
    return rr;
  }
  
  @Override
  public boolean match(final StringType type, final String value) {
    for (int i = 0; i < patterns.size(); i++) {
      if (patterns.get(i).matcher(value).find()) {
        return true;
      }
    }
    return false;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends Filter.Builder<Builder, AnyFilter> {
    @Override
    public AnyFilter build() {
      return new AnyFilter(tag, values, operator, Type.REGEXP);
    }
  }
  
}