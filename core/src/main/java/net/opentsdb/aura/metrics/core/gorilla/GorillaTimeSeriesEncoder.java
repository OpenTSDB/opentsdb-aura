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

import com.google.common.annotations.VisibleForTesting;
import net.opentsdb.aura.metrics.core.SegmentCollector;
import net.opentsdb.aura.metrics.core.TSDataConsumer;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoder;
import io.ultrabrew.metrics.Gauge;
import io.ultrabrew.metrics.MetricRegistry;

/**
 * NOTE: The leading and trailing zeros can only have up to 64 bits. Therefore
 * we're stealing the MSB of both to use as dirty and OOO flags so that when
 * we flush we can do some things a bit more efficiently.
 */
public class GorillaTimeSeriesEncoder implements TimeSeriesEncoder {

  private static final int LEADING_ZERO_LENGTH_BITS = 7;
  private static final int MEANINGFUL_BIT_LENGTH_BITS = 7;
  private static final int FIRST_TIMESTAMP_BITS = 13; // TODO: derive from the segment size
  private static int[][] TIMESTAMP_ENCODINGS = {{7, 2, 2}, {9, 6, 3}, {12, 14, 4}, {32, 15, 4}};
  private static final long LOSS_MASK = 0xFFFFFFFFFF000000l;

  protected GorillaSegment segment;
  protected final boolean lossy;

  protected long segmentCount;

  protected Gauge segmentCountGauge;
  protected String[] tags;

  protected SegmentCollector segmentCollector;

  public GorillaTimeSeriesEncoder(
      final boolean lossy,
      final MetricRegistry metricRegistry,
      final GorillaSegment segmentHandle,
      final SegmentCollector segmentCollector) {
    this.segment = segmentHandle;
    this.lossy = lossy;
    this.segmentCountGauge = metricRegistry == null ? null : metricRegistry.gauge("segment.count");
    this.segmentCollector = segmentCollector;
  }

  public void setSegment(final GorillaSegment segment) {
    this.segment = segment;
  }

  @Override
  public long createSegment(final int segmentTime) {
    segment.createSegment(segmentTime);
    segmentCount++;
    return segment.getAddress();
  }

  @Override
  public void openSegment(final long segmentAddress) {
    segment.openSegment(segmentAddress);
  }

  @Override
  public int getNumDataPoints() {
    return segment.getNumDataPoints();
  }

  @Override
  public void freeSegment() {
    segment.free();
    segmentCount--;
  }

  @Override
  public void collectSegment(final long segmentAddress) {
    segmentCollector.collect(segmentAddress);
  }

  @Override
  public void freeCollectedSegments() {
    segmentCollector.freeSegments();
  }

  @Override
  public boolean segmentIsDirty() {
    return segment.isDirty();
  }

  @Override
  public boolean segmentHasOutOfOrderOrDuplicates() {
    return segment.hasDupesOrOutOfOrderData();
  }

  @Override
  public void markSegmentFlushed() {
    segment.markFlushed();
  }

  @Override
  public int serializationLength() {
    return segment.serializationLength();
  }

  @Override
  public void serialize(byte[] buffer, int offset, int length) {
    segment.serialize(buffer, offset, length, lossy);
  }

  @Override
  public void addDataPoint(final int timestamp, final double value) {
    // *WARNING:* We don't track the width of the segment in seconds so it is
    // possible to store _MORE_ data points than expected (time wise). This can
    // then cause the read into a value buffer fail with out of bounds exceptions.
    short dataPoints = segment.getNumDataPoints();
    if (dataPoints == Short.MAX_VALUE) {
      throw new IllegalStateException("Segment has reached the capacity of "
              + Short.MAX_VALUE + " data points.");
    }
    appendTimeStamp(dataPoints, timestamp);
    appendValue(dataPoints, value);

    segment.setNumDataPoints((short) (dataPoints + 1));
    segment.updateHeader();
  }

  @Override
  public int readAndDedupe(final double[] valueBuffer) {
    segment.resetCursor();

    // NOTE: Assumption - the valueBuffer is the size of a segment in seconds. E.g.
    // 2 hours so that we index directly to the proper bucket.
    short size = segment.getNumDataPoints();
    if (size < 1) {
      // no-op. Not used in the regular path but if we use this for caching
      // it'd be useful to store empty sets.
      return 0;
    }

    int uniqueCount = 0;
    int segmentTime = segment.getSegmentTime();
    int lastTimeStamp;
    short lastTimeStampDelta;
    long lastValue;
    byte lastValueLeadingZeros = 64;
    byte lastValueTrailingZeros = 64;

    short delta = (short) segment.readData(FIRST_TIMESTAMP_BITS);
    int timestamp = delta + segmentTime;
    lastTimeStamp = timestamp;
    lastTimeStampDelta = delta;
    byte blockSize = 64;
    long l = segment.readData(blockSize);
    lastValue = l;
    double lv = Double.longBitsToDouble(l);

    int bufferLength = valueBuffer.length;
    // fist data point
    valueBuffer[lastTimeStamp - segmentTime] = lv;
    uniqueCount++;

    for (int i = 1; i < size; i++) {
      int type = findEncodingIndex(4);
      if (type > 0) {
        int index = type - 1;
        int valueBits = TIMESTAMP_ENCODINGS[index][0];
        long encodedValue = segment.readData(valueBits);
        long value = encodedValue - (1l << (valueBits - 1));
        if (value >= 0) {
          value++;
        }
        lastTimeStampDelta += value;
      }
      int nextTimestamp = lastTimeStamp + lastTimeStampDelta;
      lastTimeStamp = nextTimestamp;

      double nextValue;

      long controlValue = segment.readData(1);
      if (controlValue == 0) {
        nextValue = Double.longBitsToDouble(lastValue);
      } else {
        long blockSizeMatched = segment.readData(1);

        long xor = 0;
        if (blockSizeMatched == 0) {
          int bitsToRead = 64 - lastValueLeadingZeros - lastValueTrailingZeros;
          xor = segment.readData(bitsToRead);
          xor <<= lastValueTrailingZeros;
        } else {
          byte leadingZeros = (byte) segment.readData(LEADING_ZERO_LENGTH_BITS);
          blockSize = (byte) segment.readData(MEANINGFUL_BIT_LENGTH_BITS);
          byte trailingZeros = (byte) (64 - blockSize - leadingZeros);
          xor = segment.readData(blockSize);
          xor <<= trailingZeros;
          lastValueLeadingZeros = leadingZeros;
          lastValueTrailingZeros = trailingZeros;
        }

        long value = xor ^ lastValue;
        lastValue = value;
        nextValue = Double.longBitsToDouble(value);
      }

      int idx = nextTimestamp - segmentTime;
      if (Double.isNaN(valueBuffer[idx])) {
        uniqueCount++;
      }
      valueBuffer[idx] = nextValue;
    }
    return uniqueCount;
  }

  @Override
  public void read(final TSDataConsumer consumer) {

    segment.resetCursor();

    short size = segment.getNumDataPoints();
    if (size < 1) {
      // no-op. Not used in the regular path but if we use this for caching
      // it'd be useful to store empty sets.
      return;
    }

    int segmentTime = segment.getSegmentTime();
    int lastTimeStamp;
    short lastTimeStampDelta;
    long lastValue;
    byte lastValueLeadingZeros = 64;
    byte lastValueTrailingZeros = 64;

    short delta = (short) segment.readData(FIRST_TIMESTAMP_BITS);
    int timestamp = delta + segmentTime;
    lastTimeStamp = timestamp;
    lastTimeStampDelta = delta;
    byte blockSize = 64;
    long l = segment.readData(blockSize);
    lastValue = l;
    double lv = Double.longBitsToDouble(l);
    consumer.consume(timestamp, lv, lastTimeStampDelta); // fist data point

    for (int i = 1; i < size; i++) {
      int type = findEncodingIndex(4);
      if (type > 0) {
        int index = type - 1;
        int valueBits = TIMESTAMP_ENCODINGS[index][0];
        long encodedValue = segment.readData(valueBits);
        long value = encodedValue - (1l << (valueBits - 1));
        if (value >= 0) {
          value++;
        }
        lastTimeStampDelta += value;
      }
      int nextTimestamp = lastTimeStamp + lastTimeStampDelta;
      lastTimeStamp = nextTimestamp;

      double nextValue;

      long controlValue = segment.readData(1);
      if (controlValue == 0) {
        nextValue = Double.longBitsToDouble(lastValue);
      } else {
        long blockSizeMatched = segment.readData(1);

        long xor = 0;
        if (blockSizeMatched == 0) {
          int bitsToRead = 64 - lastValueLeadingZeros - lastValueTrailingZeros;
          xor = segment.readData(bitsToRead);
          xor <<= lastValueTrailingZeros;
        } else {
          byte leadingZeros = (byte) segment.readData(LEADING_ZERO_LENGTH_BITS);
          blockSize = (byte) segment.readData(MEANINGFUL_BIT_LENGTH_BITS);
          byte trailingZeros = (byte) (64 - blockSize - leadingZeros);
          xor = segment.readData(blockSize);
          xor <<= trailingZeros;
          lastValueLeadingZeros = leadingZeros;
          lastValueTrailingZeros = trailingZeros;
        }

        long value = xor ^ lastValue;
        lastValue = value;
        nextValue = Double.longBitsToDouble(value);
      }
      consumer.consume(nextTimestamp, nextValue, lastTimeStampDelta);
    }
  }

  @Override
  public int getSegmentTime() {
    return segment.getSegmentTime();
  }

  private void appendTimeStamp(final int dataPoints, final int timestamp) {

    int lastTimestamp = segment.getLastTimestamp();
    short lastTimestampDelta = segment.getLastTimestampDelta();
    short delta = (short) (timestamp - lastTimestamp);

    if (dataPoints == 0) { // first write
      segment.writeData(delta, FIRST_TIMESTAMP_BITS);
    } else {
      short deltaOfDelta = (short) (delta - lastTimestampDelta);

      if (deltaOfDelta == 0) {
        segment.writeData(0, 1); // writes a zero bit
        //        timestamp_0bits.getAndIncrement();
      } else {

        if (deltaOfDelta > 0) {
          // There are no zeros. Shift by one to fit in x number of bits
          deltaOfDelta--;
        }

        long absValue = Math.abs(deltaOfDelta);
        for (int i = 0; i < 4; i++) {
          int valueBits = TIMESTAMP_ENCODINGS[i][0];
          long mask = 1l << (valueBits - 1);
          if (absValue < mask) {
            int controlValue = TIMESTAMP_ENCODINGS[i][1];
            int controlBits = TIMESTAMP_ENCODINGS[i][2];

            //            TIMESTAMP_CONTROLBIT_COUNTERS[i].getAndIncrement();
            segment.writeData(controlValue, controlBits);

            // stores the signed value [-2^(n-1) to 2^n] in [0 to 2^n - 1]
            long encoded = deltaOfDelta + mask;
            segment.writeData(encoded, valueBits);
            break;
          }
        }
      }
    }
    segment.setLastTimestamp(timestamp);
    if (lastTimestampDelta != delta) {
      segment.setLastTimestampDelta(delta);
    }
  }

  private void appendValue(int dataPoints, final double value) {

    long lastValue = segment.getLastValue();
    long l = Double.doubleToRawLongBits(value);

    if (lossy) {
      l = l & LOSS_MASK; // loos last 3 bytes of mantissa
    }

    if (dataPoints == 0) { // append first value
      byte blockSize = 64;
      segment.writeData(l, 64);
      segment.setLastValue(l);
      segment.setLastValueLeadingZeros(blockSize);
      segment.setLastValueTrailingZeros(blockSize);
    } else {
      long xor = l ^ lastValue;

      // Doubles are encoded by XORing them with the previous value.  If
      // XORing results in a zero value (value is the same as the previous
      // value), only a single zero bit is stored, otherwise 1 bit is
      // stored. TODO : improve this with RLE for the number of zeros
      //
      // For non-zero XORred results, there are two choices:
      //
      // 1) If the block of meaningful bits falls in between the block of
      //    previous meaningful bits, i.e., there are at least as many
      //    leading zeros and as many trailing zeros as with the previous
      //    value, use that information for the block position and just
      //    store the XORred value.
      //
      // 2) Length of the number of leading zeros is stored in the next 5
      //    bits, then length of the XORred value is stored in the next 6
      //    bits and finally the XORred value is stored.

      if (xor == 0) {
        segment.writeData(0, 1); // writes 0 bit
        //        value_0bits.getAndIncrement();
      } else {
        byte leadingZeros = (byte) Long.numberOfLeadingZeros(xor);
        byte trailingZeros = (byte) Long.numberOfTrailingZeros(xor);
        byte meaningFullBits = (byte) (64 - leadingZeros - trailingZeros);

        byte lastValueLeadingZeros = segment.getLastValueLeadingZeros();
        byte lastValueTrailingZeros = segment.getLastValueTrailingZeros();

        if (leadingZeros == lastValueLeadingZeros && trailingZeros == lastValueTrailingZeros) {
          segment.writeData(0b10, 2); // writes 1,0. Control bit for using last block information.
          long meaningFulValue = xor >>> lastValueTrailingZeros;
          segment.writeData(meaningFulValue, meaningFullBits);
          //          value_10bits.getAndIncrement();
        } else {
          segment.writeData(
              0b11, 2); // writes 1,1. Control bit for not using last block information.
          segment.writeData(leadingZeros, LEADING_ZERO_LENGTH_BITS);
          segment.writeData(meaningFullBits, MEANINGFUL_BIT_LENGTH_BITS);
          long meaningFulValue = xor >>> trailingZeros;
          segment.writeData(meaningFulValue, meaningFullBits);

          segment.setLastValueLeadingZeros(leadingZeros);
          segment.setLastValueTrailingZeros(trailingZeros);
          //          value_11bits.getAndIncrement();
        }
      }

      if (lastValue != l) {
        segment.setLastValue(l);
      }
    }
  }

  private int findEncodingIndex(final int limit) {
    int bits = 0;
    while (bits < limit) {
      byte bit = (byte) segment.readData(1);
      if (bit == 0) {
        return bits;
      }
      bits++;
    }
    return bits;
  }

  public void reset() {
    segment.reset();
  }

  @Override
  public void collectMetrics() {
    if (segmentCountGauge != null) {
      segmentCountGauge.set(segmentCount, tags);
    }
    segment.collectMetrics();
  }

  @Override
  public void setTags(final String[] tags) {
    this.tags = tags;
    this.segment.setTags(tags);
    this.segmentCollector.setTags(tags);
  }

  @VisibleForTesting
  protected GorillaSegment getSegment() {
    return segment;
  }
}
