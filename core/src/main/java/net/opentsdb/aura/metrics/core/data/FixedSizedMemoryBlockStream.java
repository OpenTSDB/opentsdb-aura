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

package net.opentsdb.aura.metrics.core.data;

import net.opentsdb.collections.DirectArray;
import net.opentsdb.collections.DirectByteArray;
import net.opentsdb.collections.DirectLongArray;

/**
 * It's a chain of fixed sized block of memory. The address to the next block is stored at the
 * initial 8 byte of the current memory block. It takes additional 10 bytes of the first block to
 * store two additional header fields. 8 bytes for the address of the last block and 2 bytes for the
 * bit index in the last block.
 *
 * <p>The first block of the segment is shared across header and data. The {@link #header} block
 * uses a {@link DirectByteArray} and should end at a byte boundary.The {@link #dataBlock} should
 * start at a byte boundary.
 *
 * @see DirectByteArray
 * @see DirectLongArray
 */
public abstract class FixedSizedMemoryBlockStream implements MemoryBlock, BitMap {

  private static final int NEXT_BLOCK_BYTE_INDEX = 0;
  private static final int CURRENT_BLOCK_BYTE_INDEX = 8;
  private static final int CURRENT_BIT_INDEX_BYTE_INDEX = 16;
  private static final int DATA_BLOCK_ADDRESS_BITS = Long.SIZE;
  protected static final int DATA_BLOCK_ADDRESS_BYTES = Long.BYTES;

  public final int blockSizeBytes;
  public final int blockSizeLongs;
  public final int blockSizeBits;

  private long address;
  protected short bitIndex;

  private boolean atTail;
  private boolean atHead;
  protected boolean dirty;

  protected final DirectByteArray header = new DirectByteArray(0, false);
  protected final DirectLongArray dataBlock = new DirectLongArray(0, false);

  /** Cumulative count of memory blocks. */
  protected long memoryBlockCount;

  public FixedSizedMemoryBlockStream(final int blockSizeBytes) {
    this.blockSizeBytes = blockSizeBytes;
    this.blockSizeLongs = blockSizeBytes / Long.BYTES;
    this.blockSizeBits = blockSizeBytes * Byte.SIZE;
  }

  @Override
  public long allocate() {
    header.init(blockSizeBytes, false);
    address = header.getAddress();
    header.setLong(CURRENT_BLOCK_BYTE_INDEX, address);
    bitIndex = (short) (headerSizeBytes() * Byte.SIZE);
    header.setShort(CURRENT_BIT_INDEX_BYTE_INDEX, bitIndex);
    dataBlock.init(address, false, blockSizeLongs);
    memoryBlockCount++;
    atTail = true;
    atHead = false;
    return address;
  }

  @Override
  public long getAddress() {
    return address;
  }

  @Override
  public void load(final long address) {
    header.init(address, false, blockSizeBytes);
    this.address = address;
    moveToTail();
  }

  @Override
  public void write(long value, int bitsToWrite) {

    if (bitsToWrite < 1 || bitsToWrite > 64) {
      throw new IllegalArgumentException(
          String.format("Invalid bitsToWrite %d. Expected between %d to %d", bitsToWrite, 1, 64));
    }

    moveToTail();

    int longIndex = bitIndex / 64;
    byte bitShift = (byte) (bitIndex % 64);

    long v1 = value << 64 - bitsToWrite >>> bitShift;
    long v = dataBlock.get(longIndex);

    dataBlock.set(longIndex, v | v1);
    bitShift += bitsToWrite;
    if (bitShift >= 64) {
      boolean blockAdded = false;
      if (bitIndex + bitsToWrite >= blockSizeBits) { // add next block
        blockAdded = true;
        long nextBlockAddress = DirectArray.malloc(blockSizeBytes);
        dataBlock.set(0, nextBlockAddress); // store the next block address in the current block
        dataBlock.init(nextBlockAddress, false, blockSizeLongs); // point to the next block
        memoryBlockCount++;
        // store the new block address as the current block
        header.setLong(CURRENT_BLOCK_BYTE_INDEX, nextBlockAddress);
        bitIndex = DATA_BLOCK_ADDRESS_BITS;
        longIndex = 0;
      }
      bitShift -= 64;
      longIndex++;

      if (bitShift != 0) {
        long v2 = value << 64 - bitShift;
        v = dataBlock.get(longIndex);
        dataBlock.set(longIndex, v | v2);
      }
      bitIndex += bitShift;
      if (!blockAdded) {
        bitIndex += (bitsToWrite - bitShift);
      }
    } else {
      bitIndex += bitsToWrite;
    }
    dirty = true;
  }

  @Override
  public long read(final int bitsToRead) {
    if (bitsToRead < 0 || bitsToRead > 64) {
      throw new IllegalArgumentException(
          String.format("Invalid bitsToRead %d. Expected between %d to %d", bitsToRead, 0, 64));
    }

    moveToHead();

    int longIndex = bitIndex / 64;
    int bitShift = bitIndex % 64;
    long result;
    if (64 - bitShift > bitsToRead) {
      result = dataBlock.get(longIndex) << bitShift >>> 64 - bitsToRead;
      bitIndex += bitsToRead;
    } else {
      result = dataBlock.get(longIndex) << bitShift >>> bitShift;
      bitShift += bitsToRead;
      if (bitShift >= 64) {
        boolean movedToNextBlock = false;
        if (bitIndex + bitsToRead >= blockSizeBits) {
          movedToNextBlock = true;
          long nextAddress = dataBlock.get(0);
          if (nextAddress == 0) {
            throw new IllegalStateException("The address of the next block was 0.");
          }
          dataBlock.init(nextAddress, false, blockSizeLongs);
          bitIndex = DATA_BLOCK_ADDRESS_BITS;
          longIndex = 0;
        }
        bitShift -= 64;
        longIndex++;
        if (bitShift != 0) {
          result = (result << bitShift) | (dataBlock.get(longIndex) >>> 64 - bitShift);
        }
        bitIndex += bitShift;
        if (!movedToNextBlock) {
          bitIndex += (bitsToRead - bitShift);
        }
      }
    }
    return result;
  }

  @Override
  public void free() {
    moveToHead();

    long next = dataBlock.get(0);
    while (next != 0) {
      dataBlock.init(next, false, blockSizeLongs);
      next = dataBlock.get(0);
      dataBlock.free();
      memoryBlockCount--;
    }
    header.free();
    memoryBlockCount--;
  }

  protected abstract int headerSizeBytes();

  /** Moves the cursor to the beginning of the data stream. */
  public void moveToHead() {
    if (!atHead) {
      updateHeader();
      this.dataBlock.init(address, false, blockSizeLongs);
      this.bitIndex = (short) (headerSizeBytes() * Byte.SIZE);
      atHead = true;
      atTail = false;
    }
  }

  /** Moves the cursor to the end of the data stream */
  public void moveToTail() {
    if (!atTail) {
      updateHeader();
      dataBlock.init(header.getLong(CURRENT_BLOCK_BYTE_INDEX), false, blockSizeLongs);
      bitIndex = header.getShort(CURRENT_BIT_INDEX_BYTE_INDEX);
      atTail = true;
      atHead = false;
    }
  }

  public void updateHeader() {
    if (dirty) {
      header.setShort(CURRENT_BIT_INDEX_BYTE_INDEX, bitIndex);
      dirty = false;
    }
  }

  public long getMemoryBlockCount() {
    return memoryBlockCount;
  }
}
