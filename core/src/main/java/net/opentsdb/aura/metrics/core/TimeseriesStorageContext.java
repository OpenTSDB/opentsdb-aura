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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class TimeseriesStorageContext {

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  protected final int retentionHour;
  protected final int retentionSecond;
  protected final int segmentSizeHour;
  protected final StorageMode mode;
  protected final int secondsInASegment;
  protected final int segmentsInATimeSeries;
  protected final int secondsInATimeSeries;

  protected static final long SEGMENT_TIME_MASK = 0xFFFFFFFF00000000L;
  protected static final long SEGMENT_COUNT_MASK = 0x00000000FFFFFFFFL;

  public TimeseriesStorageContext(
      final int retentionHour, final int segmentSizeHour, final StorageMode mode) {
    this.mode = mode;
    this.retentionHour = retentionHour;
    this.retentionSecond = (int) TimeUnit.HOURS.toSeconds(retentionHour);
    this.segmentSizeHour = segmentSizeHour;
    this.segmentsInATimeSeries = mode.getSegmentsPerTimeseries(retentionHour, segmentSizeHour);
    this.secondsInASegment = (int) TimeUnit.HOURS.toSeconds(segmentSizeHour);
    this.secondsInATimeSeries = secondsInASegment * segmentsInATimeSeries;
  }

  public int getRetentionHour() {
    return retentionHour;
  }

  public int getSegmentSizeHour() {
    return segmentSizeHour;
  }

  public StorageMode getMode() {
    return mode;
  }

  public int getSecondsInASegment() {
    return secondsInASegment;
  }

  public int getSecondsInATimeSeries() {
    return secondsInATimeSeries;
  }

  public int getSegmentsInATimeSeries() {
    return segmentsInATimeSeries;
  }

  public int getSegmentTime(final int timeInSeconds) {
    return timeInSeconds - (timeInSeconds % secondsInASegment);
  }

  public int getRetentionSeconds() {
    return retentionSecond;
  }

  /**
   * Decode segment time from a long. Upper 32 bits represent the segment time and lower 32 bits
   * represent the segment count
   *
   * @param segmentTimes encoded value
   * @return segment time
   * @see #getSegmentTimes(int, int)
   */
  public int getFirstSegmentTime(final long segmentTimes) {
    return (int) ((segmentTimes & SEGMENT_TIME_MASK) >> 32);
  }

  /**
   * Decode segment count from a long. Upper 32 bits represent the segment time and lower 32 bits
   * represent the segment count
   *
   * @param segmentInfo encoded value
   * @return segment count
   * @see #getSegmentTimes(int, int)
   */
  public int getSegmentCount(final long segmentInfo) {
    return (int) (segmentInfo & SEGMENT_COUNT_MASK);
  }

  /**
   * Returns the segment times in the query range encoded as a 64 bit long value. Where the upper 32
   * bits represent the first segment time and lower 32 bits represent the segment count.
   *
   * @param queryStartTime start time of the query range
   * @param queryEndTime end time of the query range
   * @return encoded segment times;
   * @see #getFirstSegmentTime(long)
   * @see #getSegmentCount(long)
   */
  public abstract long getSegmentTimes(final int queryStartTime, final int queryEndTime);

//  public abstract int[] getSegmentTimes(int startTime, int endTime);
}
