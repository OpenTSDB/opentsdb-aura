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

public class OnHeapGorillaDownSampledSegment implements GorillaDownSampledSegment {

  private int segmentTime;
  private byte[] buffer;
  private int startingOffset;
  private int length;

  private int bitIndex;
  private long currentLong;
  private int byteIndex;

  private byte interval;
  private byte aggs;

  public OnHeapGorillaDownSampledSegment(
      int segmentTime, byte[] buffer, int startingOffset, int length) {
    open(segmentTime, buffer, startingOffset, length);
  }

  public void open(int segmentTime, byte[] buffer, int startingOffset, int length) {
    this.segmentTime = segmentTime;
    this.buffer = buffer;
    this.startingOffset = startingOffset;
    this.length = length;

    this.interval = buffer[1];
    this.aggs = buffer[2];
    this.bitIndex = (startingOffset + 3) * Byte.SIZE;
  }

  @Override
  public long create(int segmentTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void open(long id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void free() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getSegmentTime() {
    return segmentTime;
  }

  @Override
  public boolean isDirty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasDupesOrOutOfOrderData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markFlushed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(long value, int bitsToWrite) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long read(int bitsToRead) {

    if (bitsToRead < 0 && bitsToRead > 64) {
      throw new IllegalArgumentException(
          String.format("Invalid bitsToRead %d. Expected between %d to %d", bitsToRead, 0, 64));
    }

    int bitShift = bitIndex % Long.SIZE;
    long result;
    int remainingBits = Long.SIZE - bitShift;
    if (remainingBits > bitsToRead) {
      result = currentLong << bitShift >>> Long.SIZE - bitsToRead;
      bitIndex += bitsToRead;
    } else {
      result = currentLong << bitShift >>> bitShift;
      bitShift += bitsToRead;
      if (bitShift >= Long.SIZE) {
        if (byteIndex + Long.BYTES >= startingOffset + length) {
          currentLong = 0;
          int shifty = 56;
          while (byteIndex < startingOffset + length) {
            currentLong |= ((long) buffer[byteIndex++] & 0xFF) << shifty;
            shifty -= 8;
          }
        }else {
          currentLong = ByteArrays.getLong(buffer, byteIndex);
        }
        byteIndex += Long.BYTES;
        bitShift -= Long.SIZE;
        if (bitShift != 0) {
          result = (result << bitShift) | (currentLong >>> Long.SIZE - bitShift);
        }
        bitIndex += bitShift;
        bitIndex += (bitsToRead - bitShift);
      }
    }
    return result;
  }

  @Override
  public void setInterval(byte interval) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getInterval() {
    return interval;
  }

  @Override
  public void setAggs(byte aggId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getAggs() {
    return aggs;
  }

  @Override
  public void updateHeader() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void moveToHead() {
    this.bitIndex = 0;
    this.byteIndex = startingOffset + 3;
    this.currentLong = ByteArrays.getLong(buffer, byteIndex);
    this.byteIndex += Long.BYTES;
  }

  @Override
  public void moveToTail() {}
}
