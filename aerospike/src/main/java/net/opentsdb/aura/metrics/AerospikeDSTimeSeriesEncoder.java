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

package net.opentsdb.aura.metrics;

import net.opentsdb.aura.metrics.core.downsample.CountAggregator;
import net.opentsdb.aura.metrics.core.gorilla.GorillaDownSampledTimeSeriesEncoder;

import java.util.Arrays;

import static net.opentsdb.aura.metrics.core.downsample.DownSampledTimeSeriesEncoder.decodeInterval;
import static net.opentsdb.aura.metrics.core.downsample.DownSampledTimeSeriesEncoder.decodeSegmentWidth;

public class AerospikeDSTimeSeriesEncoder
    extends GorillaDownSampledTimeSeriesEncoder<OnHeapAerospikeSegment> {

  public AerospikeDSTimeSeriesEncoder(OnHeapAerospikeSegment segment) {
    super(
        segment.isLossy(),
        decodeInterval(segment.getInterval()),
        decodeSegmentWidth(segment.getInterval()),
        null,
        segment,
        segment.getAggs(),
        segment.getAggCount());
  }

  @Override
  public int getNumDataPoints() {
    if (!tsBitsSet) {
      segment.moveToHead();
      numPoints = segment.decodeTimestampBits(tsBitMap);
      tsBitsSet = true;
      tsBitsRead = true;
    }
    return numPoints;
  }

  @Override
  public int serializationLength() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void serialize(byte[] buffer, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void loadValueHeaders() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readAggValues(double[] valueBuffer, byte aggId) {

    if ((segment.getAggs() & aggId) == 0) {
      throw new IllegalArgumentException("aggregation with id: " + aggId + " not found");
    }
    if ((segment.getAggIdReadFromStore() & aggId) == 0) {
      throw new IllegalArgumentException("aggregation with id: " + aggId + " not fetched");
    }

    if (!tsBitsRead) {
      segment.moveToHead();
      numPoints = segment.decodeTimestampBits(tsBitMap);
      tsBitsSet = true;
      tsBitsRead = true;
    }

    if (numPoints == 0) {
      Arrays.fill(valueBuffer, Double.NaN);
      return 0;
    }

    segment.moveToAggHead(aggId);
    boolean isCount = aggId == CountAggregator.ID;
    dataPoints = 0; // reset before the read of each agg.
    for (int i = 0; i < tsBitMap.length; i++) {
      if (tsBitMap[i] == 1) {
        valueBuffer[i] = readNextValue();
      } else {
        valueBuffer[i] = isCount ? 0 : Double.NaN;
      }
    }
    return numPoints;
  }

  public int readAggValuesByIndex(double[] valueBuffer, int index) {

    if (!tsBitsRead) {
      segment.moveToHead();
      numPoints = segment.decodeTimestampBits(tsBitMap);
      tsBitsSet = true;
      tsBitsRead = true;
    }

    if (numPoints == 0) {
      Arrays.fill(valueBuffer, Double.NaN);
      return 0;
    }

    byte aggId = segment.chooseAggToRead(index);
    boolean isCount = aggId == CountAggregator.ID;
    dataPoints = 0; // reset before the read of each agg.
    for (int i = 0; i < tsBitMap.length; i++) {
      if (tsBitMap[i] == 1) {
        valueBuffer[i] = readNextValue();
      } else {
        valueBuffer[i] = isCount ? 0 : Double.NaN;
      }
    }
    return numPoints;
  }

  public static final byte encodeMapKey(final int recordOffset, final int ordinal) {
    return (byte) (recordOffset << 4 | ordinal);
  }

  public static final int decodeRecordOffset(final byte mapKey) {
    return mapKey >>> 4;
  }

  public static final int decodeAggOrdinal(final byte mapKey) {
    return mapKey & 0b00001111;
  }
}
