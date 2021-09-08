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

import io.ultrabrew.metrics.MetricRegistry;
import net.opentsdb.aura.metrics.core.OffHeapSegment;
import net.opentsdb.aura.metrics.core.data.ByteArrays;

public class OffHeapGorillaDownSampledSegment extends OffHeapSegment
    implements GorillaDownSampledSegment {

  protected static final int INTERVAL_BYTE_INDEX = 22;
  protected static final int AGG_BYTE_INDEX = 23;
  protected static final int HEADER_SIZE_BYTE = 24;

  public OffHeapGorillaDownSampledSegment(
          final int dataBlockSizeBytes) {
    super(dataBlockSizeBytes);
  }

  @Override
  public void setInterval(byte interval) {
    header.setByte(INTERVAL_BYTE_INDEX, interval);
  }

  @Override
  public byte getInterval() {
    return header.getByte(INTERVAL_BYTE_INDEX);
  }

  @Override
  public void setAggs(byte aggId) {
    header.setByte(AGG_BYTE_INDEX, aggId);
  }

  @Override
  public byte getAggs() {
    return header.getByte(AGG_BYTE_INDEX);
  }

  @Override
  public int headerLengthBytes() {
    return HEADER_SIZE_BYTE;
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
  public boolean moveToAggHead(int intervalCount) {
    if(moveToHead()) {
      bitIndex += intervalCount;
      return true;
    }
    return false;
  }

  @Override
  public void serializeBits(byte[] buffer, int offset, int bits) {

    moveToHead();

    int bitsRemaining = bits;

    long nextAddress = dataBlock.get(0);

    int longIndex = bitIndex / 64;
    int bitShift = bitIndex % 64;
    int originalOffset = offset;

    if (longIndex >= blockSizeLongs) {
      if (nextAddress == 0) {
        throw new IndexOutOfBoundsException();
      }
      dataBlock.init(nextAddress, false, blockSizeLongs);
      bitIndex = DATA_BLOCK_ADDRESS_BITS;
      nextAddress = dataBlock.get(0);
      longIndex = 1;
      bitShift = bitIndex % 64;
    }

    int bitsToCopy = 64 - bitShift;
    if (bitsToCopy <= bitsRemaining) {
      long l = dataBlock.get(longIndex++) << bitShift >>> bitShift;
//      int byteShift = (int) Math.ceil(bitShift / 8D);
      int byteShift = bitShift / 8;
      ByteArrays.putLong(l, byteShift, buffer, offset);

      bitIndex += bitsToCopy;
      bitsRemaining -= bitsToCopy;
      offset += (8 - byteShift);
    }

    while (bitsRemaining > 0) {

      if (longIndex >= blockSizeLongs) {
        if (nextAddress == 0) {
          throw new ArrayIndexOutOfBoundsException();
        }
        dataBlock.init(nextAddress, false, blockSizeLongs);
        bitIndex = DATA_BLOCK_ADDRESS_BITS;
        nextAddress = dataBlock.get(0);
        longIndex = 1;
      }

      long lv = dataBlock.get(longIndex++);
      if (bitsRemaining >= 64) {
        ByteArrays.putLong(lv, buffer, offset);

        bitIndex += 64;
        bitsRemaining -= 64;
        offset += 8;
      } else {
        int bytesToCopy = bitsRemaining / 8;
        bitsToCopy = bitsRemaining % 8;

        int shifty = 56;
        for (int i = 0; i < bytesToCopy; i++) {
          buffer[offset++] = (byte) (lv >> shifty);
          shifty -= 8;
        }

        if (bitsToCopy > 0) {
          buffer[offset++] = (byte) (lv >>> (64 - bitsRemaining) << (8 - bitsToCopy));
        }

        bitIndex += bitsRemaining;
        bitsRemaining -= bitsRemaining;
      }
    }

    // shift left to remove the extra header bits.
    bitShift %= 8;
    if (bitShift > 0) {
      shiftLeft(buffer, originalOffset, offset, bitShift);
    }
  }

  private static void shiftLeft(byte[] buffer, int start, int end, int bitShiftCount) {
    final int shiftMod = bitShiftCount % 8;
    final byte carryMask = (byte) ((1 << shiftMod) - 1);
    final int offsetBytes = (bitShiftCount / 8);

    int sourceIndex;
    for (int i = start; i < end; i++) {
      sourceIndex = i + offsetBytes;
      if (sourceIndex >= end) {
        buffer[i] = 0;
      } else {
        byte src = buffer[sourceIndex];
        byte dst = (byte) (src << shiftMod);
        if (sourceIndex + 1 < end) {
          dst |= buffer[sourceIndex + 1] >>> (8 - shiftMod) & carryMask;
        }
        buffer[i] = dst;
      }
    }
  }
}
