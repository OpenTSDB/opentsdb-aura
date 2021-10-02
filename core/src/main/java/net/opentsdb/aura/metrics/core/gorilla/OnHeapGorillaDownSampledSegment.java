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

import static net.opentsdb.aura.metrics.core.TimeSeriesEncoderType.GORILLA_LOSSY_SECONDS;

public class OnHeapGorillaDownSampledSegment extends OnHeapGorillaSegment
    implements GorillaDownSampledSegment {

  protected byte interval;
  protected byte aggs;
  protected boolean lossy;

  public OnHeapGorillaDownSampledSegment(int segmentTime, byte[] buffer) {
    this(segmentTime, buffer, 0, buffer.length);
  }

  public OnHeapGorillaDownSampledSegment(
      int segmentTime, byte[] buffer, int startingOffset, int length) {
    super(segmentTime, buffer, startingOffset, length);
    this.lossy = buffer[0] == GORILLA_LOSSY_SECONDS;
    this.interval = buffer[1];
    this.aggs = buffer[2];
    this.bitIndex = (startingOffset + 3) * Byte.SIZE;
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
  public void setAggs(byte bitMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getAggs() {
    return aggs;
  }

  public boolean isLossy() {
    return lossy;
  }

  @Override
  public boolean moveToHead() {
    this.bitIndex = 0;
    this.byteIndex = startingOffset + 3;
    this.currentLong = ByteArrays.getLong(buffer, byteIndex);
    this.byteIndex += Long.BYTES;
    return true;
  }

  @Override
  public void serializeBits(byte[] buffer, int offset, int bits) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean moveToAggHead(int intervalCount) {
    int bits = intervalCount;
    int bitShift = bits % 8;
    if (bitShift > 0) {
      // byte alignment
      bits += (8 - bitShift);
    }
    this.bitIndex = bits;
    int tsLongs = bits / 64;
    int bytesRead = startingOffset + 3 + (tsLongs * 8);
    this.currentLong = ByteArrays.getLong(buffer, bytesRead);
    this.byteIndex = bytesRead + Long.BYTES;

    return true;
  }

  @Override
  public void alignToNextByte() {
    int bitShift = bitIndex % 8;
    if (bitShift > 0) {
      read(8 - bitShift);
    }
  }
}
