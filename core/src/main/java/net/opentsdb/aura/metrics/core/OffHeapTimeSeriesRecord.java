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

import net.opentsdb.collections.DirectByteArray;

/**
 * Index layout:
 * 0 - 7 bytes = metric hash
 * 8 - 15 bytes = tag set hash
 * 16 - 16 byte = number of tags, unsigned byte I hope...
 * 17 - 20 bytes = last timestamp in epoch seconds
 * 21 - 28 bytes = last value as a long encoded double value.
 * 29 - 36 bytes = cur segment address? Or first segment address?
 */
public class OffHeapTimeSeriesRecord implements TimeSeriesRecord {

  protected static final int METRIC_KEY_INDEX = 0;
  protected static final int TAG_KEY_INDEX = METRIC_KEY_INDEX + Long.BYTES;
  protected static final int TAG_COUNT_INDEX = TAG_KEY_INDEX + Long.BYTES;
  protected static final int LAST_TIMESTAMP_INDEX = TAG_COUNT_INDEX + Byte.BYTES;
  protected static final int LAST_VALUE_INDEX = LAST_TIMESTAMP_INDEX + Integer.BYTES;
  protected static final int SEGMENT_ADDR_BASE_INDEX = LAST_VALUE_INDEX + Long.BYTES;

  protected final int recordSizeBytes;
  protected final int secondsInASegment;
  protected final int secondsInATimeSeries;

  protected DirectByteArray dataBlock;

  public OffHeapTimeSeriesRecord(
      final int segmentsInATimeSeries,
      final int secondsInASegment) {
    this.recordSizeBytes = SEGMENT_ADDR_BASE_INDEX + (Long.BYTES * segmentsInATimeSeries);
    this.secondsInASegment = secondsInASegment;
    this.secondsInATimeSeries = segmentsInATimeSeries * secondsInASegment;
    this.dataBlock = new DirectByteArray(0);
  }

  @Override
  public long create(
      final long metricKey,
      final long tagKey,
      final byte tagCount,
      final int timestamp,
      final double value,
      final int segmentTime,
      final long segmentAddress) {
    this.dataBlock.init(recordSizeBytes, false);
    setMetricKey(metricKey);
    setTagKey(tagKey);
    setTagCount(tagCount);
    setLastTimestamp(timestamp);
    setLastValue(value);
    setSegmentAddress(segmentTime, segmentAddress);
    return dataBlock.getAddress();
  }

  @Override
  public void open(final long address) {
    this.dataBlock.init(address, false, recordSizeBytes);
  }

  @Override
  public void setMetricKey(final long metricKey) {
    this.dataBlock.setLong(METRIC_KEY_INDEX, metricKey);
  }

  @Override
  public long getMetricKey() {
    return dataBlock.getLong(METRIC_KEY_INDEX);
  }

  @Override
  public void setTagKey(final long tagKey) {
    dataBlock.setLong(TAG_KEY_INDEX, tagKey);
  }

  @Override
  public long getTagKey() {
    return dataBlock.getLong(TAG_KEY_INDEX);
  }

  @Override
  public void setTagCount(final byte tagCount) {
    dataBlock.setByte(TAG_COUNT_INDEX, tagCount);
  }

  @Override
  public byte getTagCount() {
    return dataBlock.getByte(TAG_COUNT_INDEX);
  }

  @Override
  public void setLastTimestamp(final int timestamp) {
    this.dataBlock.setInt(LAST_TIMESTAMP_INDEX, timestamp);
  }

  @Override
  public int getLastTimestamp() {
    return this.dataBlock.getInt(LAST_TIMESTAMP_INDEX);
  }

  @Override
  public void setLastValue(final double value) {
    this.dataBlock.setLong(LAST_VALUE_INDEX, Double.doubleToRawLongBits(value));
  }

  @Override
  public double getLastValue() {
    return Double.longBitsToDouble(this.dataBlock.getLong(LAST_VALUE_INDEX));
  }

  @Override
  public void setSegmentAddress(final int segmentTime, final long segmentAddress) {
    int segmentIndex = getSegmentIndex(segmentTime);
    setSegmentAddressAtIndex(segmentIndex, segmentAddress);
  }

  @Override
  public long getSegmentAddress(int segmentTime) {
    int segmentIndex = getSegmentIndex(segmentTime);
    if (segmentIndex < 0) {
      return 0;
    }
    return getSegmentAddressAtIndex(segmentIndex);
  }

  @Override
  public long getAddress() {
    return dataBlock.getAddress();
  }

  @Override
  public int length() {
    return recordSizeBytes;
  }

  @Override
  public int getSegmentIndex(final int segmentTimeInSeconds) {
    return segmentTimeInSeconds % secondsInATimeSeries / secondsInASegment;
  }

  @Override
  public void setSegmentAddressAtIndex(final int segmentIndex,
                                       final long segmentAddress) {
    if (segmentIndex < 0) {
      throw new IndexOutOfBoundsException("Index cannot be negative: "
              + segmentIndex);
    }
    int offset = SEGMENT_ADDR_BASE_INDEX + (segmentIndex * Long.BYTES);
    dataBlock.setLong(offset, segmentAddress);
  }

  @Override
  public long getSegmentAddressAtIndex(final int segmentIndex) {
    if (segmentIndex < 0) {
      throw new IndexOutOfBoundsException("Index cannot be negative: "
              + segmentIndex);
    }
    int offset = SEGMENT_ADDR_BASE_INDEX + (segmentIndex * Long.BYTES);
    return dataBlock.getLong(offset);
  }

  @Override
  public void deleteSegmentAddressAtIndex(int segmentIndex) {
    if (segmentIndex < 0) {
      throw new IndexOutOfBoundsException("Index cannot be negative: "
              + segmentIndex);
    }
    int offset = SEGMENT_ADDR_BASE_INDEX + (segmentIndex * Long.BYTES);
    dataBlock.setLong(offset, 0);
  }

  @Override
  public void delete() {
    dataBlock.free();
  }
}
