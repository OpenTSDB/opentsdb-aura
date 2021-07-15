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

import net.opentsdb.aura.metrics.core.BaseSegmentEncoder;

public abstract class GorillaSegmentEncoder<T extends GorillaSegment>
    extends BaseSegmentEncoder<T> {

  protected static final int FIRST_TIMESTAMP_BITS = 13; // TODO: derive from the segment size
  protected static final int LEADING_ZERO_LENGTH_BITS = 7;
  protected static final int MEANINGFUL_BIT_LENGTH_BITS = 7;
  protected static int[][] TIMESTAMP_ENCODINGS = {{7, 2, 2}, {9, 6, 3}, {12, 14, 4}, {32, 15, 4}};
  protected static final long LOSS_MASK = 0xFFFFFFFFFF000000l;

  protected final boolean lossy;
  protected int segmentTime;
  protected short dataPoints;
  protected int lastTimestamp;
  protected short delta;
  protected short lastTimestampDelta;
  protected long newValue;
  protected long lastValue;
  protected byte blockSize;
  protected byte lastValueLeadingZeros;
  protected byte lastValueTrailingZeros;
  protected boolean meaningFullBitsChanged;

  public GorillaSegmentEncoder(final boolean lossy, final T segment) {
    super(segment);
    this.lossy = lossy;
  }

  protected int appendTimeStamp(final int timestamp) {
    int bitsWritten = 0;

    delta = (short) (timestamp - lastTimestamp);

    if (dataPoints == 0) { // first write
      segment.write(delta, FIRST_TIMESTAMP_BITS);
      bitsWritten += FIRST_TIMESTAMP_BITS;
    } else {
      short deltaOfDelta = (short) (delta - lastTimestampDelta);

      if (deltaOfDelta == 0) {
        segment.write(0, 1); // writes a zero bit
        //        timestamp_0bits.getAndIncrement();
        bitsWritten++;
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
            segment.write(controlValue, controlBits);

            // stores the signed value [-2^(n-1) to 2^n] in [0 to 2^n - 1]
            long encoded = deltaOfDelta + mask;
            segment.write(encoded, valueBits);

            bitsWritten += (controlBits + valueBits);
            break;
          }
        }
      }
    }
    lastTimestamp = timestamp;
    return bitsWritten;
  }

  /** @return number of bits written */
  protected int appendValue(final double value) {
    int bitsWritten = 0;

    newValue = Double.doubleToRawLongBits(value);

    if (lossy) {
      newValue = newValue & LOSS_MASK; // loos last 3 bytes of mantissa
    }
    if (dataPoints == 0) { // append first value
      byte blockSize = 64;
      segment.write(newValue, 64);
      lastValueLeadingZeros = blockSize;
      lastValueTrailingZeros = blockSize;

      meaningFullBitsChanged = true;
      bitsWritten += 64;
    } else {
      {
        long xor = newValue ^ lastValue;

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
          segment.write(0, 1); // writes 0 bit
          bitsWritten++;
          //        value_0bits.getAndIncrement();
        } else {
          byte leadingZeros = (byte) Long.numberOfLeadingZeros(xor);
          byte trailingZeros = (byte) Long.numberOfTrailingZeros(xor);
          byte meaningFullBits = (byte) (64 - leadingZeros - trailingZeros);

          loadValueHeaders();

          if (leadingZeros == lastValueLeadingZeros && trailingZeros == lastValueTrailingZeros) {
            segment.write(0b10, 2); // writes 1,0. Control bit for using last block information.
            long meaningFulValue = xor >>> lastValueTrailingZeros;
            segment.write(meaningFulValue, meaningFullBits);
            //          value_10bits.getAndIncrement();
            bitsWritten += (2 + meaningFullBits);
          } else {
            segment.write(0b11, 2); // writes 1,1. Control bit for not using last block information.
            segment.write(leadingZeros, LEADING_ZERO_LENGTH_BITS);
            segment.write(meaningFullBits, MEANINGFUL_BIT_LENGTH_BITS);
            long meaningFulValue = xor >>> trailingZeros;
            segment.write(meaningFulValue, meaningFullBits);

            meaningFullBitsChanged = true;

            lastValueLeadingZeros = leadingZeros;
            lastValueTrailingZeros = trailingZeros;
            //          value_11bits.getAndIncrement();
            bitsWritten +=
                (2 + LEADING_ZERO_LENGTH_BITS + MEANINGFUL_BIT_LENGTH_BITS + meaningFullBits);
          }
        }
      }
    }
    dataPoints++;

    return bitsWritten;
  }

  protected int readNextTimestamp() {
    int timestamp;
    if (dataPoints == 0) {
      short delta = (short) segment.read(FIRST_TIMESTAMP_BITS);
      timestamp = delta + segmentTime;
      lastTimestamp = timestamp;
      lastTimestampDelta = delta;
    } else {
      int type = findEncodingIndex(4);
      if (type > 0) {
        int index = type - 1;
        int valueBits = TIMESTAMP_ENCODINGS[index][0];
        long encodedValue = segment.read(valueBits);
        long value = encodedValue - (1l << (valueBits - 1));
        if (value >= 0) {
          value++;
        }
        lastTimestampDelta += value;
      }
      timestamp = lastTimestamp + lastTimestampDelta;
      lastTimestamp = timestamp;
    }
    return timestamp;
  }

  protected double readNextValue() {
    double dv;
    if (dataPoints == 0) {
      lastValueLeadingZeros = 64;
      lastValueTrailingZeros = 64;
      blockSize = 64;

      long l = segment.read(blockSize);
      lastValue = l;
      dv = Double.longBitsToDouble(l);
    } else {
      long controlValue = segment.read(1);
      if (controlValue == 0) {
        return Double.longBitsToDouble(lastValue);
      } else {
        long blockSizeMatched = segment.read(1);

        long xor;
        if (blockSizeMatched == 0) {
          int bitsToRead = 64 - lastValueLeadingZeros - lastValueTrailingZeros;
          xor = segment.read(bitsToRead);
          xor <<= lastValueTrailingZeros;
        } else {
          byte leadingZeros = (byte) segment.read(LEADING_ZERO_LENGTH_BITS);
          blockSize = (byte) segment.read(MEANINGFUL_BIT_LENGTH_BITS);
          byte trailingZeros = (byte) (64 - blockSize - leadingZeros);
          xor = segment.read(blockSize);
          xor <<= trailingZeros;
          lastValueLeadingZeros = leadingZeros;
          lastValueTrailingZeros = trailingZeros;
        }

        long value = xor ^ lastValue;
        lastValue = value;
        dv = Double.longBitsToDouble(value);
      }
    }
    dataPoints++;
    return dv;
  }

  protected abstract void loadValueHeaders();

  protected int findEncodingIndex(final int limit) {
    int bits = 0;
    while (bits < limit) {
      byte bit = (byte) segment.read(1);
      if (bit == 0) {
        return bits;
      }
      bits++;
    }
    return bits;
  }
}
