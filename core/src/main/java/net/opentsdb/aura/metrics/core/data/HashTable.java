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

/**
 * Ensure the size is at least 30% larger than expected max size. Note: This will take 10 * size
 * bytes of memory for the allocated size + the size of actual entries stored. I.e. if you allocate
 * 1M entries, 10M bytes will be used and when you write e.g. 10 1MB entries, additional 10MB will
 * be used. Maximum size is approximately 214M entries. An exception will be thrown on
 * initialization if the size is too large.
 */
public class HashTable {

  private final LinearProbingHashTable lookupTable;
  private int sz;
  public static final int valSz = 12;

  private byte[] pointerBuffer = new byte[valSz];

  /**
   * @param name
   * @param size number of entries. WARNING: use ~30% larger value than expected max size to avoid
   */
  public HashTable(String name, int size) {
    lookupTable = new LinearProbingHashTable(size, valSz, name);
  }

  //Need not be public
  private HashTable(LinearProbingHashTable lookupTable) {
    this.lookupTable = lookupTable;
    this.sz = lookupTable.size();
  }

  public void put(long key, byte[] data) {
    put(key, data, 0, data.length);
  }

  public void put(long key, byte[] data, final int offset, final int length) {
    byte[] pointer = lookupTable.get(key, pointerBuffer);

    if (pointer != null) {
      update(key, pointer, data);
    } else {
      sz++;
      long addr = Memory.malloc(length);

      pointer = pointerBuffer;
      ByteArrays.putLong(addr, pointer, 0);
      ByteArrays.putInt(length, pointer, 8);

      Memory.write(addr, data, offset, length);
      lookupTable.set(key, pointer);
    }
  }

  /**
   * @param pointer from the read. This is updated to point to the new location.
   */
  public void update(long key, byte[] pointer, byte[] data) {
    long oldAddr = ByteArrays.getLong(pointer, 0);
    int oldLength = ByteArrays.getInt(pointer, 8);

    if (oldLength != data.length) {
      Memory.free(oldAddr);
      long addr = Memory.malloc(data.length);
      Memory.write(addr, data, data.length);

      ByteArrays.putLong(addr, pointer, 0);
      ByteArrays.putInt(data.length, pointer, 8);

      lookupTable.set(key, pointer);
    } else {
      Memory.write(oldAddr, data, data.length);
    }
  }

  /**
   * @return pointer array and data array. Null if not found
   */
  public byte[][] get2(long key) {
    byte[] pointer = lookupTable.get(key);

    if (pointer == null) {
      return null;
    }

    long addr = ByteArrays.getLong(pointer, 0);
    int len = ByteArrays.getInt(pointer, 8);

    // TODO - re-use the byte arrays. This is generating a fair bit of
    // garbage.
    byte[] buf = new byte[len];
    Memory.read(addr, buf, len);
    return new byte[][]{pointer, buf};
  }

  /**
   * @return pointer array and data array. Null if not found
   */
  public boolean get2(long key, byte[][] buffer) {
    byte[] pointer = lookupTable.get(key, buffer[0]);

    if (pointer == null) {
      return false;
    }

    long addr = ByteArrays.getLong(pointer, 0);
    int len = ByteArrays.getInt(pointer, 8);
    Memory.read(addr, buffer[1], len);

    return true;
  }

  public byte[] get(long key) {
    byte[] pointer = lookupTable.get(key, pointerBuffer);

    if (pointer == null) {
      return null;
    }

    long addr = ByteArrays.getLong(pointer, 0);
    int len = ByteArrays.getInt(pointer, 8);

    // TODO - re-use the byte arrays. This is generating a fair bit of
    // garbage.
    byte[] buf = new byte[len];
    Memory.read(addr, buf, len);
    return buf;
  }

  public boolean getPointer(final long key, final byte[] buffer) {
    return lookupTable.get(key, buffer) != null;
  }

  public boolean getPointer2(final long key, final byte[] buffer) {
    return lookupTable.get2(key, buffer) != null;
  }

  public byte[] get(long key, byte[] buf) {
    byte[] pointer = lookupTable.get(key, pointerBuffer);

    if (pointer == null) {
      return null;
    }

    long addr = ByteArrays.getLong(pointer, 0);
    int len = ByteArrays.getInt(pointer, 8);

    Memory.read(addr, buf, len);
    return buf;
  }

  public boolean containsKey(long key) {
    return lookupTable.get(key, pointerBuffer) != null;
  }

  public int size() {
    return sz;
  }

  public int length() {
    return lookupTable.length();
  }

  public byte[][] values() {
    return lookupTable.values();
  }

  public void readSlotIndex(final int[] result) throws InSufficientBufferLengthException {
    lookupTable.readSlotIndex(result);
  }

  public void readPointerDataAtSlot(final int slotIndex, final byte[] pointerBuffer) {
    lookupTable.readValue(slotIndex, pointerBuffer);
  }

  /**
   * @return byte array containing addr, len and slot index. It gives direct access to the slot
   * for better performance.
   */
  public byte[][] metaEntries() {
    return lookupTable.metaEntries();
  }

  public void resetSlot(int slotOffset) {
    if (lookupTable.resetSlot(slotOffset)) {
      sz--;
    }
  }

  /**
   * @return byte array containing key, address, length and slot index. It gives direct access to
   *     the slot for better performance.
   */
  public byte[][] entries() {
    return lookupTable.entries();
  }

  /**
   * @return a HashTable, which is a clone of this one.
   */
  public HashTable clone() {

    final LinearProbingHashTable inner_clone = this.lookupTable.clone();
    return new HashTable(inner_clone);
  }

}
