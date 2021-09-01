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

package net.opentsdb.aura.metrics.core;

import io.ultrabrew.metrics.MetricRegistry;
import net.opentsdb.aura.metrics.core.data.InSufficientBufferLengthException;
import net.opentsdb.aura.metrics.meta.MetaQuery;
import net.opentsdb.aura.metrics.meta.MetricQuery;
import net.opentsdb.aura.metrics.meta.SharedMetaResult;
import net.opentsdb.data.LowLevelMetricData;

public interface TimeSeriesShardIF {

  int NOT_FOUND = -1;

  int DOCID_BATCH_SIZE = 256000 / Long.BYTES; // L2 cache aligned.

  int getId();

  void addEvent(LowLevelMetricData.HashedLowLevelMetricData event);

  void purge();

  void read(int startTime, int endTime, long timeSeriesHash, TSDataConsumer consumer);

  long query(MetricQuery query, int firstSegmentTime, int segmentCount) throws Exception;

  long query(MetricQuery query) throws Exception;

  void readAll(long timeSeriesHash, TSDataConsumer consumer);

  SharedMetaResult queryMeta(MetaQuery query);

  byte getTagCount(long timeSeriesKey);

  boolean getTagPointer(long tagKey, byte[] buffer);

  void submit(Runnable job) throws InterruptedException;

  int getAndRemoveTagset(long tagKey, byte[] byteBuffer) throws InSufficientBufferLengthException;

  long getDataPointCount();

  long getDataDropCount();

  void flush();

  MetricRegistry metricRegistry();
}
