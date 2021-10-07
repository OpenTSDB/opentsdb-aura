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

import net.opentsdb.aura.metrics.core.downsample.DownSampledSegment;

public interface GorillaDownSampledSegment extends GorillaSegment, DownSampledSegment {

  void serializeBits(byte[] buffer, int offset, int bits);

  boolean moveToAggHead(int intervalCount);

  default int decodeTimestampBits(byte[] buffer) {
    int intervalCount = buffer.length;
    int longs = intervalCount / 64;
    int leftOver = intervalCount % 64;
    int offset = 0;
    int index = 0;
    int numPoints = 0;

    for (int i = 0; i < longs; i++) {
      long bitMap = read(64);
      while (++offset <= 64) {
        boolean isBitSet = (bitMap & (1l << (64 - offset))) != 0;
        if (isBitSet) {
          buffer[index++] = 1;
          numPoints++;
        } else {
          buffer[index++] = 0;
        }
      }
      offset = 0;
    }

    if (leftOver > 0) {
      long bitMap = read(leftOver);
      bitMap = bitMap << 64 - leftOver;
      while (++offset <= leftOver) {
        boolean isBitSet = (bitMap & (1l << (64 - offset))) != 0;
        if (isBitSet) {
          buffer[index++] = 1;
          numPoints++;
        } else {
          buffer[index++] = 0;
        }
      }

      // Byte alignment
      int remainingBits = 64 - leftOver;
      int bitsToAlign = remainingBits % 8;
      if (bitsToAlign > 0) {
        read(bitsToAlign);
      }
    }
    return numPoints;
  }

  void alignToNextByte();
}
