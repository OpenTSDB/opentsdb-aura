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

public interface BasicTimeSeriesEncoder extends TimeSeriesEncoder, LazyStatsCollector {

  void addDataPoint(int timestamp, double value);

  void read(TSDataConsumer consumer);

  /**
   * Decodes the segments, sorts and removes the duplicate data points. Subsequent data points will override the previous values.
   * It offsets the timestamp from the segment time and uses that as the index in the array to store the values.
   * So, the length of the array should be equal to the number of seconds in the segment. For two hour segment, the array should be of length 7200.
   *
   * @param valueBuffer values array of size == seconds in the segment.
   * @return count of unique data points
   */
  int readAndDedupe(double[] valueBuffer);

  void collectSegment(long segmentAddress);

  void freeCollectedSegments();

  boolean segmentIsDirty();

  boolean segmentHasOutOfOrderOrDuplicates();

  void markSegmentFlushed();

}
