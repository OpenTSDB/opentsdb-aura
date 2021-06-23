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

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;

import java.util.Map;

public class MatchAllFilter extends Filter {

  private final int CLASS_NAME_HASH = getClass().getName().hashCode();

  protected MatchAllFilter() {
    super(null, null, null, null);
  }

  @Override
  public RoaringBitmap apply(Map<String, Map<String, RoaringBitmap>> indexMap) {
    RoaringBitmap filterRR = new RoaringBitmap(); // empty bitmap
    for (Map.Entry<String, Map<String, RoaringBitmap>> entryMap : indexMap.entrySet()) {
      final RoaringBitmap fastAgg = FastAggregation.or(entryMap.getValue().values().iterator());
      filterRR = FastAggregation.or(fastAgg, filterRR); 
    }
    return filterRR;
  }

  public boolean equals(Object other) {
    return other != null && getClass() == other.getClass();
  }

  public int hashCode() {
    return CLASS_NAME_HASH;
  }

  @Override
  public String toString() {
    return ".*";
  }
  
  @Override
  public boolean match(final StringType type, final String value) {
    return true;
  }
  
}