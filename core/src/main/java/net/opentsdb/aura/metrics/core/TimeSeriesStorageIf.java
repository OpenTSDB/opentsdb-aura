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

import net.opentsdb.aura.metrics.meta.MetricQuery;
import net.opentsdb.data.LowLevelMetricData;

public interface TimeSeriesStorageIf {

  void addEvent(LowLevelMetricData.HashedLowLevelMetricData event);

  /**
   * @param query the metric query
   * @param start query start time
   * @param end query end time
   * @param consumer records the shardId and the pointer to the resultant array.
   * @return a encoded long value, where the upper 32 bits represent the first segment time and
   *     lower 32 bits represent the segment count.
   */
  long read(MetricQuery query, int start, int end, ResultConsumer consumer);

  void readLastValue(MetricQuery query, ResultConsumer consumer);

  int numShards();

  int getSegmentHour(int timeInSeconds);

  int retentionInHours();

  int getShardId(long hash);

  TimeSeriesShardIF getShard(int shardId);

  int secondsInSegment();

  long getDataPointCount();

  long getDataDropCount();

  TimeseriesStorageContext getContext();
}
