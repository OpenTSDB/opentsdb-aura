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

package net.opentsdb.aura.metrics.storage;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult.GroupResult.TagHashes;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An ID wherein the tags are long hashes mapped to a meta query result table.
 */
public class AuraHashedID implements TimeSeriesId, TimeSeriesStringId {

  private final String metric;
  private final MetaTimeSeriesQueryResult metaQueryResult;
  private final TagHashes tagHashes;
  private Map<String, String> tags;

  public AuraHashedID(final String metric,
                      final MetaTimeSeriesQueryResult metaQueryResult,
                      final TagHashes tagHashes) {
    this.metric = metric;
    this.metaQueryResult = metaQueryResult;
    this.tagHashes = tagHashes;
  }

  @Override
  public boolean encoded() {
    return false;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> type() {
    return Const.TS_STRING_ID;
  }

  @Override
  public long buildHashCode() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public String alias() {
    return null;
  }

  @Override
  public String namespace() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public String metric() {
    return metric;
  }

  @Override
  public Map<String, String> tags() {
    // TODO - we can get fancier if the TagHashes interface resets on iteration.
    if (tags == null) {
      // load
      tags = Maps.newHashMap();
      String key = null;
      for (int i = 0; i < tagHashes.size(); i++) {
        if (i % 2 == 0) {
          key = metaQueryResult.getStringForHash(tagHashes.next());
        } else {
          String value = metaQueryResult.getStringForHash(tagHashes.next());
          tags.put(key, value);
        }
      }
    }
    return tags;
  }

  @Override
  public String getTagValue(String s) {
    return null;
  }

  @Override
  public List<String> aggregatedTags() {
    return null;
  }

  @Override
  public List<String> disjointTags() {
    return null;
  }

  @Override
  public Set<String> uniqueIds() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public long hits() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int compareTo(TimeSeriesStringId o) {
    throw new UnsupportedOperationException("TODO");
  }
}