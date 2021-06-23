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

/**
 * A length encoded array to store the pointers to the timeseries and Tags data stored off heap. A
 * single slot is of (8 + 8 + 4) bytes, stores address of a timeseries data, address of the
 * corresponding tag data and the length of the tag data. Timeseries data is of fixed length. So, it
 * need not store the length of the timeseries data. 4 extra bytes to store the timeseries count.
 */
public class ResultantPointerArray implements DirectArray {

  /**
   * A single slot is of 20 (8 + 8 + 4) bytes, stores address of a timeseries data, address of the
   * corresponding tag data and the length of the tag data
   */
  public static int SLOT_SIZE = 8 + 8 + 4;

  /**
   * 4 extra bytes reserved at the beginning of the array. It stores the total time series count,
   */
  private static int TS_COUNT_BYTES = Integer.BYTES;

  /** time series count is stored at the first logical 4 bytes of the underlying byte array. */
  private static int TS_COUNT_BYTE_INDEX = 0;

  private int capacity;

  private DirectByteArray byteArray;

  /**
   * Creates a {@link ResultantPointerArray} with the given <code>capacity</code>.
   *
   * @param capacity length of the array
   */
  public ResultantPointerArray(final int capacity) {
    this.capacity = capacity;
    this.byteArray = new DirectByteArray(capacity > 0 ? TS_COUNT_BYTES + capacity * SLOT_SIZE : 0);
  }

  /**
   * Creates a {@link ResultantPointerArray} from a previously allocated memory. It reads the first
   * 4 bytes from the start address and store that value in {@link ResultantPointerArray#capacity}
   *
   * @param startAddress address to the allocated memory.
   */
  public ResultantPointerArray(final long startAddress) {
    this.byteArray = new DirectByteArray(startAddress);
    int capacityBytes = byteArray.getCapacity();
    this.capacity = (capacityBytes - TS_COUNT_BYTES) / SLOT_SIZE;
  }

  /**
   * Recreates the array with the new <code>capacity</code>. It allocates a new set of bytes off
   * heap but reuses the Object instance on heap. This api is provided to reuse the object instance
   * of {@link ResultantPointerArray} and minimize the garbage collection pressure under high load.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM
   *
   * @param capacity length of the array
   * @return the address to the previously allocated memory.
   */
  public long create(final int capacity) {
    this.capacity = capacity;
    return byteArray.init(capacity > 0 ? TS_COUNT_BYTES + capacity * SLOT_SIZE : 0);
  }

  /**
   * Recreates the array with a previously allocated memory. Resets the {@link
   * ResultantPointerArray#capacity} field by reading the first 4 bytes from the <code>startAddress
   * </code>. This api is provided to reuse the object instance of {@link ResultantPointerArray} and
   * minimize the garbage collection pressure under high load.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM.
   *
   * @param startAddress address to the allocated memory.
   * @return the address to the previously allocated memory.
   */
  public long reuse(final long startAddress) {
    long oldStartAddress = byteArray.init(startAddress);
    int capacityBytes = byteArray.getCapacity();
    this.capacity = (capacityBytes - TS_COUNT_BYTES) / SLOT_SIZE;
    return oldStartAddress;
  }

  @Override
  public int getCapacity() {
    return capacity;
  }

  @Override
  public long getAddress() {
    return byteArray.getAddress();
  }

  @Override
  public void free() {
    this.byteArray.free();
    this.capacity = 0;
  }

  public void set(
      final int slotIndex, final long tsAddress, final long tagAddress, final int tagLength) {
    int baseOffset = byteIndex(slotIndex);
    byteArray.setLong(baseOffset, tsAddress);
    byteArray.setLong(baseOffset + 8, tagAddress);
    byteArray.setInt(baseOffset + 16, tagLength);
  }

  public void setTSCount(final int count) {
    byteArray.setInt(TS_COUNT_BYTE_INDEX, count);
  }

  public int getTSCount() {
    return capacity <= 0 ? 0 : byteArray.getInt(TS_COUNT_BYTE_INDEX);
  }

  public long getTSAddress(final int slotIndex) {
    int baseOffset = byteIndex(slotIndex);
    return byteArray.getLong(baseOffset);
  }

  public long getTagAddress(final int slotIndex) {
    int baseOffset = byteIndex(slotIndex);
    return byteArray.getLong(baseOffset + 8);
  }

  public int getTagLength(final int slotIndex) {
    int baseOffset = byteIndex(slotIndex);
    return byteArray.getInt(baseOffset + 16);
  }

  /**
   * Does the boundary check and offsets the index from the {@code startAddress}
   *
   * @param slotIndex <code>index</code> of the array being accessed.
   * @return index of the byte array being accessed.
   * @throws IndexOutOfBoundsException if {@code slotIndex < 0 || slotIndex >= capacity}
   */
  private int byteIndex(int slotIndex) {
    if ((slotIndex < 0) || (slotIndex >= capacity)) {
      throw new IndexOutOfBoundsException(String.valueOf(slotIndex));
    }
    return TS_COUNT_BYTES + slotIndex * SLOT_SIZE;
  }
}
