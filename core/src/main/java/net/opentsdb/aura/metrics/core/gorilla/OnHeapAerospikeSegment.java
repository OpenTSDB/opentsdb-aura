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

public class OnHeapAerospikeSegment extends OnHeapGorillaDownSampledSegment {
  private byte[] header;
  private int headerOffset;
  private int headerLength;
  private byte aggId;
  private byte[] agg;
  private int aggCount;

  public OnHeapAerospikeSegment(int segmentTime, byte[] header, byte aggId, byte[] agg) {
    super(segmentTime, header, 0, header.length);
    this.header = header;
    this.headerOffset = 0;
    this.headerLength = header.length;
    this.aggId = aggId;
    this.agg = agg;
    this.bitIndex = 0;
    this.aggCount = 1;
  }

  @Override
  public boolean moveToHead() {
    this.buffer = header;
    this.length = headerLength;
    return super.moveToHead();
  }

  @Override
  public boolean moveToAggHead(int intervalCount) {
    return false;
  }

  public void moveToAggHead(byte aggId) {
    this.bitIndex = 0;
    this.buffer = agg;
    this.length = agg.length;
    this.currentLong = ByteArrays.getLong(buffer, 0);
    this.byteIndex = Long.BYTES;
  }

  public byte getAggID() {
    return aggId;
  }

  public int getAggCount() {
    return aggCount;
  }
}
