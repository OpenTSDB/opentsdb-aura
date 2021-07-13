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
public class OnHeapGorillaSegment implements BasicGorillaSegment {
  static final int NUM_POINTS_OFFSET = 1;

  private int segmentTime;
  private byte[] buffer;
  private int startingOffset;
  private int longIndex;
  private int length;
  int numDataPoints;

  int bitIndex;
  long previousLong;

  public OnHeapGorillaSegment() {
    // make sure to use the reset method.
  }

  public OnHeapGorillaSegment(int segmentTime, byte[] buffer, int startingOffset, int length) {
    this.segmentTime = segmentTime;
    this.buffer = buffer;
    this.startingOffset = startingOffset;
    this.length = length;
  }

  public void reset(int segmentTime, byte[] buffer, int startingOffset, int length) {
    this.segmentTime = segmentTime;
    this.buffer = buffer;
    this.startingOffset = startingOffset;
    this.length = length;
    bitIndex = 0;
    previousLong = 0;
  }

  @Override
  public long create(int segmentTime) {
    return 0;
  }

  @Override
  public void open(long segmentAddress) {}

  @Override
  public void free() {}

  @Override
  public boolean isDirty() {
    return false;
  }

  @Override
  public boolean hasDupesOrOutOfOrderData() {
    return false;
  }

  @Override
  public void markFlushed() {}

  @Override
  public int getSegmentTime() {
    return segmentTime;
  }

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
  public long read(int bitsToRead) {
    if (bitsToRead < 0 && bitsToRead > 64) {
      throw new IllegalArgumentException(
          String.format("Invalid bitsToRead %d. Expected between %d to %d", bitsToRead, 0, 64));
    }

    long result = 0;
    int bitShift = bitIndex % Long.SIZE;
    if (Long.SIZE - bitShift > bitsToRead) {
      result = previousLong << bitShift >>> Long.SIZE - bitsToRead;
      bitIndex += bitsToRead;
    } else {
      result = previousLong << bitShift >>> bitShift;
      bitShift += bitsToRead;
      if (bitShift >= Long.SIZE) {
        // need the next long.
        if (longIndex + Long.BYTES >= startingOffset + length) {
          // we can't read out a long as there were trailing zero bytes so
          // we need to shift manually
          int shifty = 56;
          previousLong = 0;
          while (longIndex < startingOffset + length) {
            previousLong |= ((long) buffer[longIndex++] & 0xFF) << shifty;
            shifty -= 8;
          }
        } else {
          previousLong = ByteArrays.getLong(buffer, longIndex);
        }
        longIndex += Long.BYTES;
        bitShift -= Long.SIZE;
        if (bitShift != 0) {
          result = (result << bitShift) | (previousLong >>> Long.SIZE - bitShift);
        }
        bitIndex += bitShift;
        bitIndex += (bitsToRead - bitShift);
      }
    }
    return result;
  }

  @Override
  public void write(long value, int bitsToWrite) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateHeader() {}

  @Override
  public void moveToTail() {}

  @Override
  public void moveToHead() {
    bitIndex = 0;
    // Number of data points is variable length encoded on either 1 or 2 bytes.
    if ((OffHeapGorillaSegment.TWO_BYTE_FLAG & buffer[startingOffset + NUM_POINTS_OFFSET]) != 0) {
      numDataPoints =
          (buffer[startingOffset + NUM_POINTS_OFFSET] & OffHeapGorillaSegment.TWO_BYTE_MASK) << 8;
      numDataPoints |= (buffer[startingOffset + NUM_POINTS_OFFSET + 1] & 0xFF);
      longIndex = startingOffset + 3;
    } else {
      numDataPoints = buffer[startingOffset + NUM_POINTS_OFFSET];
      longIndex = startingOffset + 2;
    }
    if (numDataPoints < 1) {
      return;
    }
    previousLong = ByteArrays.getLong(buffer, longIndex);
    longIndex += Long.BYTES;
  }

  @Override
  public int serializationLength() {
    return 0;
  }

  @Override
  public void serialize(byte[] buffer, int offset, int length, boolean lossy) {}

  @Override
  public void collectMetrics() {

  }

  @Override
  public void setTags(String[] tags) {

  }
}
