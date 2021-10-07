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

import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.gorilla.OnHeapGorillaDownSampledSegment;

import static net.opentsdb.aura.metrics.core.TimeSeriesEncoderType.GORILLA_LOSSY_SECONDS;

public class OnHeapAerospikeSegment extends OnHeapGorillaDownSampledSegment {

  private byte[] header;
  private int headerLength;
  private int aggCount;
  private byte[] aggIds;
  private byte[][] aggs;
  private byte aggId;

  public OnHeapAerospikeSegment(int segmentTime, byte[] header, byte[] aggIds, byte[][] aggs) {
    super(segmentTime, header);
    reset(header, aggIds, aggs);
  }

  public void reset(int segmentTime, byte[] header, byte[] aggIds, byte[][] aggs) {
    this.segmentTime = segmentTime;
    reset(header, aggIds, aggs);
    this.lossy = header[0] == GORILLA_LOSSY_SECONDS;
    this.interval = header[1];
    this.aggBitMap = header[2];
  }

  private void reset(byte[] header, byte[] aggIds, byte[][] aggs) {
    this.header = header;
    this.headerLength = header.length;
    this.bitIndex = 0;
    this.aggIds = aggIds;
    this.aggs = aggs;
    this.aggCount = aggs.length;
    this.aggId = 0;
    for (int i = 0; i < aggIds.length; i++) {
      this.aggId |= aggIds[i];
    }
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

  protected void moveToAggHead(byte aggId) {
    int aggIndex = getAggIndex(aggId);
    chooseAggToRead(aggIndex);
  }

  public byte getAggIdReadFromStore() {
    return aggId;
  }

  private int getAggIndex(byte aggId) {
    for (int i = 0; i < aggIds.length; i++) {
      if (aggIds[i] == aggId) {
        return i;
      }
    }
    return -1;
  }

  protected byte chooseAggToRead(int index) {
    this.bitIndex = 0;
    this.buffer = aggs[index];
    this.length = buffer.length;
    this.currentLong = ByteArrays.getLong(buffer, 0);
    this.byteIndex = Long.BYTES;
    return aggIds[index];
  }

  public int getAggCount() {
    return aggCount;
  }
}
