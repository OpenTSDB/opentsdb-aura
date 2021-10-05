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
import net.opentsdb.aura.metrics.core.downsample.AggregationLengthIterator;
import net.opentsdb.aura.metrics.core.downsample.AggregatorIterator;
import net.opentsdb.aura.metrics.core.downsample.CountAggregator;
import net.opentsdb.aura.metrics.core.downsample.DownSampledTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.downsample.DownSampler;
import net.opentsdb.aura.metrics.core.downsample.Interval;
import net.opentsdb.aura.metrics.core.downsample.SegmentWidth;

import java.util.Arrays;

import static net.opentsdb.aura.metrics.core.downsample.DownSampledTimeSeriesEncoder.encodeInterval;

public class GorillaDownSampledTimeSeriesEncoder<T extends GorillaDownSampledSegment>
    extends GorillaSegmentEncoder<T> implements DownSampledTimeSeriesEncoder {

  private short intervalCount;
  private Interval interval;
  private SegmentWidth segmentWidth;
  private DownSampler downSampler;
  protected byte aggId;
  private int aggCount;
  private int[] aggLengthInBits;
  protected int numPoints;
  private int intervalCountBytes;

  private AggregationLengthIteratorImpl iterator;
  protected byte[] tsBitMap;
  protected boolean tsBitsSet;
  protected boolean tsBitsRead;

  public GorillaDownSampledTimeSeriesEncoder(
      final boolean lossy,
      final Interval interval,
      final SegmentWidth segmentWidth,
      final DownSampler downSampler,
      final T segment) {
    this(
        lossy,
        interval,
        segmentWidth,
        downSampler,
        segment,
        downSampler.getAggId(),
        downSampler.getAggCount());
  }

  public GorillaDownSampledTimeSeriesEncoder(
      final boolean lossy,
      final Interval interval,
      final SegmentWidth segmentWidth,
      final DownSampler downSampler,
      final T segment,
      final byte aggId,
      final int aggCount) {

    super(lossy, segment);
    this.interval = interval;
    this.segmentWidth = segmentWidth;
    this.downSampler = downSampler;
    this.aggId = aggId;
    this.aggCount = aggCount;
    this.aggLengthInBits = new int[aggCount];
    this.intervalCount = (short) (segmentWidth.getSeconds() / interval.getSeconds());
    this.intervalCountBytes = (int) Math.ceil(intervalCount / Byte.SIZE);
    this.tsBitMap = new byte[intervalCount];
  }

  @Override
  public long createSegment(int segmentTime) {
    long address = super.createSegment(segmentTime);
    segment.setInterval(encodeInterval(interval, segmentWidth));
    segment.setAggs(aggId);
    tsBitsSet = false;
    tsBitsRead = false;
    return address;
  }

  @Override
  public void openSegment(long id) {
    super.openSegment(id);
    tsBitsSet = false;
    tsBitsRead = false;
  }

  @Override
  public int getNumDataPoints() {
    if (tsBitsSet) {
      return numPoints;
    } else {
      throw new IllegalStateException("Need to decode the timestamp bitmaps");
    }
  }

  @Override
  public int getIntervalCount() {
    return intervalCount;
  }

  @Override
  public void addDataPoints(final double[] rawValues) {

    downSampler.apply(rawValues);

    AggregatorIterator<double[]> iterator = downSampler.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      double[] aggs = iterator.next();

      if (!tsBitsSet) {
        numPoints = addTimestamps(aggs);
        tsBitsSet = true;
      }

      int numBits = addAggregation(aggs, iterator.aggID());
      aggLengthInBits[i++] = numBits;
    }
    segment.updateHeader();
  }

  @Override
  public int readAggValues(final double[] valueBuffer, final byte aggId) {

    if ((this.aggId & aggId) == 0) { // agg not found
      throw new IllegalArgumentException("aggregation with id: " + aggId + " not found");
    }

    if (!tsBitsRead) {
      segment.moveToHead();
      numPoints = segment.decodeTimestampBits(tsBitMap);
      tsBitsSet = true;
      tsBitsRead = true;
    } else {
      segment.moveToAggHead(intervalCount);
    }

    if (numPoints == 0) {
      Arrays.fill(valueBuffer, Double.NaN);
      return 0;
    }

    AggregationLengthIterator iterator = aggIterator();
    boolean isCount = aggId == CountAggregator.ID;
    while (iterator.hasNext()) {
      iterator.next();
      dataPoints = 0; // reset before the read of each agg.
      if (aggId == iterator.aggID()) {
        for (int i = 0; i < tsBitMap.length; i++) {
          if (tsBitMap[i] == 1) {
            valueBuffer[i] = readNextValue();
          } else {
            valueBuffer[i] = isCount ? 0 : Double.NaN;
          }
        }
        break;
      } else {
        for (int i = 0; i < numPoints; i++) {
          readNextValue();
        }
        segment.alignToNextByte();
      }
    }

    return numPoints;
  }

  //  private int decodeTimeStamps(final double[] valueBuffer) {
  //
  //    int longs = intervalCount / 64;
  //    int leftOver = intervalCount % 64;
  //    int offset = 0;
  //    int index = 0;
  //    int numPoints = 0;
  //
  //    for (int i = 0; i < longs; i++) {
  //      long bitMap = segment.read(64);
  //      while (++offset <= 64) {
  //        boolean isBitSet = (bitMap & (1l << (64 - offset))) != 0;
  //        if (isBitSet) {
  //          valueBuffer[index++] = 0.0;
  //          numPoints++;
  //        } else {
  //          valueBuffer[index++] = Double.NaN;
  //        }
  //      }
  //      offset = 0;
  //    }
  //
  //    if (leftOver > 0) {
  //      long bitMap = segment.read(leftOver);
  //      bitMap = bitMap << 64 - leftOver;
  //      while (++offset <= leftOver) {
  //        boolean isBitSet = (bitMap & (1l << (64 - offset))) != 0;
  //        if (isBitSet) {
  //          valueBuffer[index++] = 0.0;
  //          numPoints++;
  //        } else {
  //          valueBuffer[index++] = Double.NaN;
  //        }
  //      }
  //    }
  //    return numPoints;
  //  }

  @Override
  public int getAggCount() {
    return aggCount;
  }

  @Override
  public Interval getInterval() {
    return interval;
  }

  @Override
  public SegmentWidth getSegmentWidth() {
    return segmentWidth;
  }

  @Override
  public AggregationLengthIterator aggIterator() {
    if (iterator == null) {
      iterator = new AggregationLengthIteratorImpl();
    }
    iterator.reset();
    return iterator;
  }

  private int addTimestamps(final double[] aggs) {
    // encode timestamp bits.
    long bitMap = 0;
    int offset = 0;
    int numPoints = 0;
    final int lastIndex = aggs.length - 1;
    for (int i = 0; i < aggs.length; i++) {
      offset++;
      if (!Double.isNaN(aggs[i])) {
        bitMap = bitMap | (1l << (64 - offset));
        numPoints++;
        tsBitMap[i] = 1;
      } else {
        tsBitMap[i] = 0;
      }

      if (offset == 64 || i == lastIndex) {
        if (offset < 64) {
          int remainingBits = 64 - offset;
          int bitShift = remainingBits % 8;
          remainingBits -= bitShift; // byte aligned.
          bitMap = bitMap >>> remainingBits;
          offset += bitShift;
        }
        segment.write(bitMap, offset);

        bitMap = 0;
        offset = 0;
      }
    }
    return numPoints;
  }

  private int addAggregation(final double[] aggs, final byte aggId) {
    dataPoints = 0;
    int bitsWritten = 0;
    for (int i = 0; i < aggs.length; i++) {
      double value = aggs[i];
      if (!Double.isNaN(value) && !(aggId == CountAggregator.ID && value <= 0)) {
        bitsWritten += appendValue(value);
        lastValue = newValue;
      }
    }

    // Byte alignment
    int bitShift = bitsWritten % 8;
    if (bitShift > 0) {
      int bitsToAlign = 8 - bitShift;
      segment.write(0, bitsToAlign);
      bitsWritten += bitsToAlign;
    }
    return bitsWritten;
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
    buffer[offset++] = segment.getInterval();
    buffer[offset++] = segment.getAggs();

    length -= 3;

    segment.serialize(segment.headerLengthBytes(), buffer, offset, length);
  }

  @Override
  public int serializeHeader(byte[] buffer, int offset) {

    buffer[offset++] =
        lossy
            ? TimeSeriesEncoderType.GORILLA_LOSSY_SECONDS
            : TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS;
    buffer[offset++] = segment.getInterval();
    buffer[offset++] = segment.getAggs();

    int tsBits = intervalCount;
    int tsBytes = intervalCountBytes;
    segment.serialize(segment.headerLengthBytes(), buffer, offset, tsBytes);

    int bitShift = tsBits % Byte.SIZE;
    if (bitShift > 0) {
      // unset the extra trailing bits
      final byte mask = (byte) (0xFF << Byte.SIZE - bitShift);
      buffer[offset + tsBytes - 1] &= mask;
    }
    return 3 + tsBytes;
  }

  @Override
  protected void loadValueHeaders() {
    // doNothing
  }

  private class AggregationLengthIteratorImpl implements AggregationLengthIterator {

    AggregatorIterator aggIterator;
    int index = -1;

    public AggregationLengthIteratorImpl() {
      aggIterator = downSampler.iterator();
    }

    @Override
    public int aggLengthInBits() {
      if (tsBitsSet) {
        return aggLengthInBits[index];
      } else {
        // TODO decode the segment
        throw new UnsupportedOperationException("need to decode the agg values");
      }
    }

    @Override
    public void serialize(byte[] buffer, int offset) {
      int bits = aggLengthInBits();
      segment.serializeBits(buffer, offset, bits);
    }

    @Override
    public int aggOrdinal() {
      return aggIterator.aggOrdinal();
    }

    @Override
    public byte aggID() {
      return aggIterator.aggID();
    }

    @Override
    public String aggName() {
      return aggIterator.aggName();
    }

    @Override
    public void reset() {
      aggIterator.reset();
      index = -1;
    }

    @Override
    public boolean hasNext() {
      return aggIterator.hasNext();
    }

    @Override
    public Void next() {
      index++;
      aggIterator.next();
      return null;
    }
  }
}
