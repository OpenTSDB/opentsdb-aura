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

import net.opentsdb.aura.metrics.core.data.ByteArrays;

/** More work to do on this but it's meant to deserialize stored gorilla segments. */
public class OnHeapGorillaRawSegment extends OnHeapGorillaSegment implements GorillaRawSegment {

  static final int NUM_POINTS_OFFSET = 1;
  int numDataPoints;

  public OnHeapGorillaRawSegment() {
    this(0, null, 0, 0);
  }

  public OnHeapGorillaRawSegment(int segmentTime, byte[] buffer, int startingOffset, int length) {
    super(segmentTime, buffer, startingOffset, length);
  }

  @Override
  public void free() {}

  @Override
  public void setNumDataPoints(short numDataPoints) {}

  @Override
  public short getNumDataPoints() {
    return (short) numDataPoints;
  }

  @Override
  public void setLastTimestamp(int lastTimestamp) {}

  @Override
  public int getLastTimestamp() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLastValue(long value) {}

  @Override
  public long getLastValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLastTimestampDelta(short lastTimestampDelta) {}

  @Override
  public short getLastTimestampDelta() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLastValueLeadingZeros(byte lastLeadingZero) {}

  @Override
  public byte getLastValueLeadingZeros() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLastValueTrailingZeros(byte lastTrailingZero) {}

  @Override
  public byte getLastValueTrailingZeros() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean moveToHead() {
    bitIndex = 0;
    // Number of data points is variable length encoded on either 1 or 2 bytes.
    if ((OffHeapGorillaRawSegment.TWO_BYTE_FLAG & buffer[startingOffset + NUM_POINTS_OFFSET])
        != 0) {
      numDataPoints =
          (buffer[startingOffset + NUM_POINTS_OFFSET] & OffHeapGorillaRawSegment.TWO_BYTE_MASK)
              << 8;
      numDataPoints |= (buffer[startingOffset + NUM_POINTS_OFFSET + 1] & 0xFF);
      byteIndex = startingOffset + 3;
    } else {
      numDataPoints = buffer[startingOffset + NUM_POINTS_OFFSET];
      byteIndex = startingOffset + 2;
    }

    if(numDataPoints >= 1) {
      currentLong = ByteArrays.getLong(buffer, byteIndex);
      byteIndex += Long.BYTES;
    }
    return true;
  }

  @Override
  public void collectMetrics() {}

  @Override
  public void setTags(String[] tags) {}
}
