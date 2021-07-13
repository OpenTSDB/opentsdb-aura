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

import io.ultrabrew.metrics.Gauge;
import io.ultrabrew.metrics.MetricRegistry;
import net.opentsdb.aura.metrics.core.OffHeapSegment;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderType;
import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.collections.DirectByteArray;
import net.opentsdb.collections.DirectLongArray;

public class OffHeapGorillaSegment extends OffHeapSegment implements BasicGorillaSegment {

  protected static final int LAST_TIMESTAMP_BYTE_INDEX = 22;
  protected static final int LAST_VALUE_BYTE_INDEX = 26;
  protected static final int NUM_DATA_POINT_BYTE_INDEX = 34;
  protected static final int LAST_TIMESTAMP_DELTA_BYTE_INDEX = 36;
  protected static final int LAST_LEADING_ZERO_BYTE_INDEX = 38;
  protected static final int LAST_TRAILING_ZERO_BYTE_INDEX = 39;

  private static final int HEADER_SIZE_LONGS = 5;
  private static final int HEADER_SIZE_BYTES = HEADER_SIZE_LONGS * Long.BYTES;

  public static final byte TWO_BYTE_FLAG = (byte) 0x80;
  public static final byte TWO_BYTE_MASK = (byte) 0x7F;
  protected static final byte ZEROS_FLAG = (byte) 0x80;
  protected static final byte ZEROS_MASK = (byte) 0x7F;

  protected boolean dirty;
  protected boolean ooo;
  protected boolean readyToRead;

  protected Gauge memoryBlockCountGauge;
  protected Gauge segmentLengthGauge;
  protected String[] tags;

  public OffHeapGorillaSegment(final int dataBlockSizeBytes, final MetricRegistry metricRegistry) {
    super(dataBlockSizeBytes);
    this.memoryBlockCountGauge = metricRegistry.gauge("memory.block.count");
    this.segmentLengthGauge = metricRegistry.gauge("segment.length");
  }

  @Override
  public long create(int segmentTime) {
    long address = super.create(segmentTime);
    header.setInt(LAST_TIMESTAMP_BYTE_INDEX, segmentTime);
    return address;
  }

  @Override
  public void open(final long segmentAddress) {
    super.open(segmentAddress);
    // set dirty and ooo
    dirty = (header.getByte(LAST_LEADING_ZERO_BYTE_INDEX) & ZEROS_FLAG) != 0;
    ooo = (header.getByte(LAST_TRAILING_ZERO_BYTE_INDEX) & ZEROS_FLAG) != 0;
  }

  @Override
  public int getSegmentTime() {
    return header.getInt(SEGMENT_TIME_BYTE_INDEX);
  }

  @Override
  public void setNumDataPoints(final short numDataPoints) {
    header.setShort(NUM_DATA_POINT_BYTE_INDEX, numDataPoints);
  }

  @Override
  public short getNumDataPoints() {
    return header.getShort(NUM_DATA_POINT_BYTE_INDEX);
  }

  @Override
  public void setLastTimestamp(final int lastTimestamp) {
    if (!ooo && getNumDataPoints() >= 1 && lastTimestamp <= getLastTimestamp()) {
      ooo = true;
    }
    header.setInt(LAST_TIMESTAMP_BYTE_INDEX, lastTimestamp);
  }

  @Override
  public int getLastTimestamp() {
    return header.getInt(LAST_TIMESTAMP_BYTE_INDEX);
  }

  @Override
  public void setLastValue(final long value) {
    header.setLong(LAST_VALUE_BYTE_INDEX, value);
  }

  @Override
  public long getLastValue() {
    return header.getLong(LAST_VALUE_BYTE_INDEX);
  }

  @Override
  public void setLastTimestampDelta(final short lastTimestampDelta) {
    header.setShort(LAST_TIMESTAMP_DELTA_BYTE_INDEX, lastTimestampDelta);
  }

  @Override
  public short getLastTimestampDelta() {
    return header.getShort(LAST_TIMESTAMP_DELTA_BYTE_INDEX);
  }

  @Override
  public void setLastValueLeadingZeros(final byte lastLeadingZero) {
    header.setByte(LAST_LEADING_ZERO_BYTE_INDEX, (byte) (lastLeadingZero | ZEROS_FLAG));
  }

  @Override
  public byte getLastValueLeadingZeros() {
    return (byte) (header.getByte(LAST_LEADING_ZERO_BYTE_INDEX) & ZEROS_MASK);
  }

  @Override
  public void setLastValueTrailingZeros(final byte lastTrailingZero) {
    byte encoded = lastTrailingZero;
    if (ooo) {
      encoded |= ZEROS_FLAG;
    }
    header.setByte(LAST_TRAILING_ZERO_BYTE_INDEX, encoded);
  }

  @Override
  public byte getLastValueTrailingZeros() {
    return (byte) (header.getByte(LAST_TRAILING_ZERO_BYTE_INDEX) & ZEROS_MASK);
  }

  DirectByteArray getHeader() {
    return header;
  }

  DirectLongArray getDataBlock() {
    return dataBlock;
  }

  short getBitIndex() {
    return bitIndex;
  }

  @Override
  public void write(long value, int bitsToWrite) {
    super.write(value, bitsToWrite);
    if (!dirty) {
      header.setByte(
          LAST_LEADING_ZERO_BYTE_INDEX, (byte) (getLastValueLeadingZeros() | ZEROS_FLAG));
      dirty = true;
    }
  }

  //  @Override
  //  public long read(final int bitsToRead) {
  //    if (!readyToRead) {
  //      throw new IllegalStateException("Segment is not in read mode. Call resetCursor().");
  //    }
  //    if (bitsToRead < 0 || bitsToRead > 64) {
  //      throw new IllegalArgumentException(
  //          String.format("Invalid bitsToRead %d. Expected between %d to %d", bitsToRead, 0, 64));
  //    }
  //
  //    int longIndex = bitIndex / 64;
  //    int bitShift = bitIndex % 64;
  //    long result;
  //    if (64 - bitShift > bitsToRead) {
  //      result = dataBlock.get(longIndex) << bitShift >>> 64 - bitsToRead;
  //      bitIndex += bitsToRead;
  //    } else {
  //      result = dataBlock.get(longIndex) << bitShift >>> bitShift;
  //      bitShift += bitsToRead;
  //      if (bitShift >= 64) {
  //        boolean movedToNextBlock = false;
  //        if (bitIndex + bitsToRead >= blockSizeBits) {
  //          movedToNextBlock = true;
  //          long nextAddress = dataBlock.get(0);
  //          if (nextAddress == 0) {
  //            throw new IllegalStateException("The address of the next block was 0.");
  //          }
  //          dataBlock.init(nextAddress, false, blockSizeLongs);
  //          bitIndex = DATA_BLOCK_ADDRESS_BITS;
  //          longIndex = 0;
  //        }
  //        bitShift -= 64;
  //        longIndex++;
  //        if (bitShift != 0) {
  //          result = (result << bitShift) | (dataBlock.get(longIndex) >>> 64 - bitShift);
  //        }
  //        bitIndex += bitShift;
  //        if (!movedToNextBlock) {
  //          bitIndex += (bitsToRead - bitShift);
  //        }
  //      }
  //    }
  //    return result;
  //  }

  //  @Override
  //  public void write(final long value, final int bitsToWrite) {
  //    if (readyToRead) {
  //      throw new IllegalStateException("Segment is not in write mode. Re-open the " +
  //              "segment or set the bit index to the header's index.");
  //    }
  //    if (bitsToWrite < 1 || bitsToWrite > 64) {
  //      throw new IllegalArgumentException(
  //          String.format("Invalid bitsToWrite %d. Expected between %d to %d", bitsToWrite, 1,
  // 64));
  //    }
  //
  //    if (!dirty) {
  //      header.setByte(LAST_LEADING_ZERO_BYTE_INDEX,
  //              (byte) (getLastValueLeadingZeros() | ZEROS_FLAG));
  //      dirty = true;
  //    }
  //
  //    int longIndex = bitIndex / 64;
  //    byte bitShift = (byte) (bitIndex % 64);
  //
  //    long v1 = value << 64 - bitsToWrite >>> bitShift;
  //    long v = dataBlock.get(longIndex);
  //
  //    dataBlock.set(longIndex, v | v1);
  //    bitShift += bitsToWrite;
  //    if (bitShift >= 64) {
  //      boolean blockAdded = false;
  //      if (bitIndex + bitsToWrite >= blockSizeBits) { // add next block
  //        blockAdded = true;
  //        long nextBlockAddress = DirectArray.malloc(blockSizeBytes);
  //        dataBlock.set(0, nextBlockAddress); // store the next block address in the current block
  //        dataBlock.init(nextBlockAddress, false, blockSizeLongs); // point to the next block
  //        memoryBlockCount++;
  //        // store the new block address as the current block
  //        header.setLong(CURRENT_DATA_BLOCK_BYTE_INDEX, nextBlockAddress);
  //        bitIndex = DATA_BLOCK_ADDRESS_BITS;
  //        longIndex = 0;
  //      }
  //      bitShift -= 64;
  //      longIndex++;
  //
  //      if (bitShift != 0) {
  //        long v2 = value << 64 - bitShift;
  //        v = dataBlock.get(longIndex);
  //        dataBlock.set(longIndex, v | v2);
  //      }
  //      bitIndex += bitShift;
  //      if (!blockAdded) {
  //        bitIndex += (bitsToWrite - bitShift);
  //      }
  //    } else {
  //      bitIndex += bitsToWrite;
  //    }
  //  }

  //  @Override
  //  public void updateHeader() {
  //    super.updateHeader();
  //    setCurrentBitIndex(bitIndex);
  //  }

  @Override
  protected int headerSizeBytes() {
    return HEADER_SIZE_BYTES;
  }

  @Override
  public boolean isDirty() {
    return dirty;
  }

  @Override
  public boolean hasDupesOrOutOfOrderData() {
    return ooo;
  }

  @Override
  public void markFlushed() {
    dirty = false;
    header.setByte(LAST_LEADING_ZERO_BYTE_INDEX, getLastValueLeadingZeros());
  }

  @Override
  public void collectMetrics() {
    this.memoryBlockCountGauge.set(memoryBlockCount, tags);
    this.segmentLengthGauge.set(memoryBlockCount * blockSizeBytes, tags);
  }

  @Override
  public void setTags(final String[] tags) {
    this.tags = tags;
  }

  @Override
  public int serializationLength() {
    int bytes = 1; // type
    if (getNumDataPoints() <= 127) {
      bytes++;
    } else {
      bytes += 2;
    }

    int headerRemaining = (blockSizeBytes - (HEADER_SIZE_LONGS * Long.BYTES));
    long nextAddress = header.getAddress();

    int finalBytes = (int) Math.ceil((double) bitIndex / 8D);
    while (nextAddress != 0) {
      dataBlock.init(nextAddress, false, blockSizeLongs);
      nextAddress = dataBlock.get(0);
      if (nextAddress == 0) {
        bytes += finalBytes;
        if (dataBlock.getAddress() == header.getAddress()) {
          bytes -= (HEADER_SIZE_LONGS * Long.BYTES);
        } else {
          bytes -= DATA_BLOCK_ADDRESS_BYTES;
        }
        break;
      } else if (dataBlock.getAddress() == header.getAddress()) {
        bytes += headerRemaining + 1;
        continue;
      }

      bytes += blockSizeBytes - DATA_BLOCK_ADDRESS_BYTES;
    }
    return bytes;
  }

  @Override
  public void serialize(byte[] buffer, int offset, int length, boolean lossy) {
    int bitIndex = this.bitIndex;
    moveToHead();

    int index = offset;
    buffer[index++] =
        lossy
            ? TimeSeriesEncoderType.GORILLA_LOSSY_SECONDS
            : TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS;

    if (getNumDataPoints() <= 127) {
      buffer[index++] = (byte) getNumDataPoints();
    } else {
      // a bit messier. we have a signed short for the len but the first bit
      // is 1 to denote we have a 2 byte num dps.
      short dps = getNumDataPoints();
      buffer[index] = (byte) (dps >> 8);
      buffer[index++] |= TWO_BYTE_FLAG;
      buffer[index++] = (byte) dps;
    }

    int blockIndex = HEADER_SIZE_LONGS;
    long nextAddress = dataBlock.get(0);
    int finalBytes = (int) Math.ceil((double) bitIndex / 8D) / Long.BYTES;
    while (blockIndex < blockSizeLongs) {
      if (nextAddress == 0 && blockIndex > finalBytes) {
        // no more data in this final block.
        break;
      }
      long lv = dataBlock.get(blockIndex++);
      if (index + 8 >= offset + length) {
        int shifty = 56;
        while (index < offset + length) {
          buffer[index++] = (byte) (lv >> shifty);
          shifty -= 8;
        }
      } else {
        ByteArrays.putLong(lv, buffer, index);
        index += 8;
      }

      if (blockIndex >= blockSizeLongs) {
        if (nextAddress == 0) {
          break;
        }
        dataBlock.init(nextAddress, false, blockSizeLongs);
        nextAddress = dataBlock.get(0);
        blockIndex = 1;
      }
    }
  }
}
