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

import com.google.common.reflect.TypeToken;
import net.opentsdb.aura.metrics.core.BasicTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuraMetricsNumericIterator implements TypedTimeSeriesIterator<NumericType> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuraMetricsNumericIterator.class);

  private final MutableNumericValue dp;
  private final MutableNumericValue next_dp;
  private TimeStamp local_timestamp = new SecondTimeStamp(0);

  int segmentIndex = 0;
  int dpsRead;
  int dpsInSegment;
  int query_start;
  int query_end;
  boolean has_next;
  boolean initialized;
  private int segmentTime;
  private final int segmentCount;
  private final int secondsInSegment;

  TimeSeriesRecord timeSeriesRecord;
  BasicTimeSeriesEncoder timeSeriesEncoder;
  int i = 0;
  final AuraMetricsNumericArrayIterator.Accumulator accumulator;

  public AuraMetricsNumericIterator(
      final QueryNode node,
      final long tsPointer,
      final int firstSegmentTime,
      final int segmentCount) {

    TimeSeriesStorageIf timeSeriesStorage = ((AuraMetricsSourceFactory) node.factory()).timeSeriesStorage();
    this.secondsInSegment = timeSeriesStorage.secondsInSegment();
    // TODO - gag!!
    if (AuraMetricsNumericArrayIterator.threadLocalAccs == null) {
      synchronized (AuraMetricsNumericIterator.class) {
        if (AuraMetricsNumericArrayIterator.threadLocalAccs == null) {
          AuraMetricsNumericArrayIterator.threadLocalAccs = ThreadLocal.withInitial(() -> new AuraMetricsNumericArrayIterator.Accumulator(secondsInSegment));
        }
      }
    }
    accumulator = AuraMetricsNumericArrayIterator.threadLocalAccs.get();

    dp = new MutableNumericValue();
    next_dp = new MutableNumericValue();

    query_start = (int) node.pipelineContext().query().startTime().epoch();
    query_end = (int) node.pipelineContext().query().endTime().epoch();
    this.segmentCount = segmentCount;

    // find the first value within query range.
    this.segmentTime = firstSegmentTime;
    while (segmentIndex < segmentCount) {
      if ((segmentTime + secondsInSegment) > query_start && (segmentTime + secondsInSegment) <= query_end) {
        break;
      }
      segmentTime += secondsInSegment;
      segmentIndex++;
    }

    if (segmentIndex >= segmentCount) {
      // no data so just bail.
      return;
    }

    timeSeriesRecord = AuraMetricsNumericArrayIterator.threadLocalTimeseries.get();
    timeSeriesRecord.open(tsPointer);
    timeSeriesEncoder = AuraMetricsNumericArrayIterator.threadTSEncoder.get();

    while (has_next = advance()) {
      if (next_dp.timestamp().epoch() >= query_start) {
        break;
      }
    }
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    dp.reset(next_dp);
    has_next = advance();
    return dp;
  }

  @Override
  public TypeToken<NumericType> getType() {
    return NumericType.TYPE;
  }

  @Override
  public void close() {
    accumulator.reset();
  }

  boolean advance() {
    while (true) {
      if (dpsRead >= dpsInSegment) {
        if (initialized) {
          segmentIndex++;
          segmentTime += secondsInSegment;
        } else {
          initialized = true;
        }
        if (segmentTime >= query_end) { // query end time is exclusive
          return false;
        }

        long segmentAddress = timeSeriesRecord.getSegmentAddress(segmentTime);
        if (segmentAddress > 0) {
          timeSeriesEncoder.openSegment(segmentAddress);
          if (segmentTime == timeSeriesEncoder.getSegmentTime()) {
            accumulator.reset();
            accumulator.baseTimeStamp = segmentTime;
            dpsInSegment = timeSeriesEncoder.readAndDedupe(accumulator.values);
            dpsRead = 0;
            i = 0;
          } else {
            dpsRead = dpsInSegment; // force a rollover
            continue;
          }
        } else {
          dpsRead = dpsInSegment; // force a rollover
          continue;
        }
      } else {
        break;
      }
    }

    double[] values = accumulator.values;
    double value = values[i++];
    while (Double.isNaN(value) && (i < values.length)) {
      value = values[i++];
    }

    int timeStamp = accumulator.baseTimeStamp + i - 1;
    if (timeStamp >= query_end) {
      return false;
    }
    local_timestamp.updateEpoch(timeStamp);
    next_dp.reset(local_timestamp, value);
    dpsRead++;
    return true;
  }
}
