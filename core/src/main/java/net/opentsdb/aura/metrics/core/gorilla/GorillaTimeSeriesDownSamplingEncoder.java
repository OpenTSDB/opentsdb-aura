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

package net.opentsdb.aura.metrics.core.gorilla;

import net.opentsdb.aura.metrics.core.TimeSeriesEncoderType;
import net.opentsdb.aura.metrics.core.downsample.DownSampler;
import net.opentsdb.aura.metrics.core.downsample.DownSamplingTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.downsample.Interval;
import net.opentsdb.aura.metrics.core.downsample.SegmentWidth;

import java.util.Arrays;
import java.util.Iterator;

public class GorillaTimeSeriesDownSamplingEncoder
    extends BaseGorillaSegmentEncoder<OffHeapDownSampledGorillaSegment>
    implements DownSamplingTimeSeriesEncoder {

  private short intervalCount;
  private Interval interval;
  private SegmentWidth segmentSize;
  private DownSampler downSampler;
  private byte aggId;

  public GorillaTimeSeriesDownSamplingEncoder(
      final boolean lossy,
      final Interval interval,
      final SegmentWidth segmentWidth,
      final DownSampler downSampler,
      final OffHeapDownSampledGorillaSegment segment) {

    super(lossy, segment);
    this.interval = interval;
    this.segmentSize = segmentWidth;
    this.downSampler = downSampler;
    this.aggId = downSampler.getAggId();
    this.intervalCount = (short) (segmentWidth.getWidth() / interval.getWidth());
  }

  @Override
  public long createSegment(int segmentTime) {
    long address = super.createSegment(segmentTime);
    segment.setInterval(encodeInterval(interval, segmentSize));
    segment.setAggs(aggId);
    return address;
  }

  private static byte encodeInterval(Interval interval, SegmentWidth segmentSize) {
    return (byte) (interval.getId() << 3 | segmentSize.getId());
  }

  static int decodeIntervalCount(byte encoded) {
    int intervalInSeconds = Interval.getById((byte) (encoded >>> 3)).getWidth();
    int secondsInRawSegment = SegmentWidth.getById((byte) (encoded & 0b111)).getWidth();
    return secondsInRawSegment / intervalInSeconds;
  }

  @Override
  public int getNumDataPoints() {
    return intervalCount;
  }

  @Override
  public void addDataPoints(final double[] rawValues) {

    downSampler.apply(rawValues);

    Iterator<double[]> iterator = downSampler.iterator();
    boolean addTime = true;
    while (iterator.hasNext()) {
      double[] aggs = iterator.next();

      if (addTime) {
        addTimestamps(aggs);
        addTime = false;
      }

      addAggregation(aggs);
    }

    segment.updateHeader();
  }

  @Override
  public int readAggValues(final double[] valueBuffer, final byte aggId) {

    if ((segment.getAggs() & aggId) == 0) { // agg not found
      throw new IllegalArgumentException("aggregation with id: " + aggId + " not found");
    }

    segment.moveToHead();

    int longs = intervalCount / 64;
    int leftOver = intervalCount % 64;
    int offset = 0;
    int index = 0;
    int numPoints = 0;

    for (int i = 0; i < longs; i++) {
      long bitMap = segment.read(64);
      while (++offset <= 64) {
        boolean isBitSet = (bitMap & (1l << (64 - offset))) != 0;
        if (isBitSet) {
          valueBuffer[index++] = 0.0;
          numPoints++;
        } else {
          valueBuffer[index++] = Double.NaN;
        }
      }
      offset = 0;
    }

    if (leftOver > 0) {
      long bitMap = segment.read(leftOver);
      bitMap = bitMap << 64 - leftOver;
      while (++offset <= leftOver) {
        boolean isBitSet = (bitMap & (1l << (64 - offset))) != 0;
        if (isBitSet) {
          valueBuffer[index++] = 0.0;
          numPoints++;
        } else {
          valueBuffer[index++] = Double.NaN;
        }
      }
    }

    if (numPoints == 0) {
      Arrays.fill(valueBuffer, Double.NaN);
      return 0;
    }

    dataPoints = 0;
    index = -1;
    for (int i = 0; i < numPoints; i++) {
      double v = readNextValue();
      while (Double.isNaN(valueBuffer[++index]))
        ;
      valueBuffer[index] = v;
    }

    return numPoints;
  }

  private void addTimestamps(final double[] aggs) {
    // encode timestamp bits.
    long bitMap = 0;
    int offset = 0;
    final int lastIndex = aggs.length - 1;
    for (int i = 0; i < aggs.length; i++) {
      offset++;
      if (!Double.isNaN(aggs[i])) {
        bitMap = bitMap | (1l << (64 - offset));
      }

      if (offset == 64 || i == lastIndex) {
        if (offset < 64) {
          bitMap = bitMap >>> 64 - offset;
        }
        segment.write(bitMap, offset);

        bitMap = 0;
        offset = 0;
      }
    }
  }

  private void addAggregation(final double[] aggs) {
    dataPoints = 0;
    for (int i = 0; i < aggs.length; i++) {
      appendValue(aggs[i]);
      lastValue = newValue;
    }
  }

  @Override
  public int serializationLength() {
    return segment.dataLengthBytes() + 3; // 1 byte each for type, interval and aggs;
  }

  @Override
  public void serialize(byte[] buffer, int offset, int length) {
    buffer[offset++] =
        lossy
            ? TimeSeriesEncoderType.GORILLA_LOSSY_SECONDS
            : TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS;

    // includes 2 bytes, interval and aggs from the header
    segment.serialize(segment.headerLengthBytes() - 2, buffer, offset, length);
  }

  @Override
  protected void loadValueHeaders() {
    // doNothing
  }
}
