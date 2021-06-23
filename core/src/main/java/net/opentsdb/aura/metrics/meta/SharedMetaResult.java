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

public interface SharedMetaResult {

  void reset(MetaQuery query);

  MetaQuery query();

  void incrementHits(long cardinality);

  int addString(String metric, int cardinality);

  int addTimeSeries(String metric, int count);

  void addTimeSeriesTags(long tagAddress, int tagLength);

  void updateCount(int countIndex, int count);

  int addString(String string, int cardinality, boolean incrementHits);

  void addPairKey(String key, long cardinality, int count);

  void addPairValue(String value, int cardinality);
}
