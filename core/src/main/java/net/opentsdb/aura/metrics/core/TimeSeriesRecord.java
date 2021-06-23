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

public interface TimeSeriesRecord extends DataBlock {

  long create(
      long metricKey,
      long tagKey,
      byte tagSetCount,
      int timestamp,
      double value,
      int segmentTime,
      long segmentAddress);

  void open(long address);

  void setMetricKey(long metricKey);

  long getMetricKey();

  void setTagKey(long tagKey);

  long getTagKey();

  void setTagCount(byte tagCount);

  byte getTagCount();

  void setLastTimestamp(int timestamp);

  int getLastTimestamp();

  void setLastValue(double value);

  double getLastValue();

  void setSegmentAddress(int segmentTime, long segmentAddress);

  long getSegmentAddress(int segmentTime);

  int getSegmentIndex(int segmentTime);

  void setSegmentAddressAtIndex(int segmentIndex, long segmentAddress);

  long getSegmentAddressAtIndex(int segmentIndex);

  // NOTE: Does not release the memory held by the segment, only resets the
  // address in the record to 0.
  void deleteSegmentAddressAtIndex(int segmentIndex);

  void delete();
}
