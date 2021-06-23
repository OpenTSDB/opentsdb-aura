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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

/**
 * A hashtable for constant length keys and values with auto expand based on scan length.
 *
 * Key must be hashed already! The key with value 0 indicates an empty slot and -1 indicates a slot to be reset and ready to reuse.
 */
public class LinearProbingHashTable {

    private static Logger logger = LoggerFactory.getLogger(LinearProbingHashTable.class);

    private static final int MAXIMUM_CAPACITY = 1 << 30;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private static final int KEY_SZ = 8;
    private final int valSz;
    private final int slotSz;
    private byte[] table;
    private int slots;
    private int sz;
    private int threshold;
    private String name;

    private byte[] keyBuffer = new byte[KEY_SZ];
    private byte[] valueBuffer;

    public LinearProbingHashTable(int size, int valSz, String name) {
        this.valSz = valSz;
        this.valueBuffer = new byte[valSz];
        this.slotSz = KEY_SZ + valSz;
        this.slots = size;
        this.name = name;

        resize();
        logger.info("{} size: {}", name, table.length);
    }

    public LinearProbingHashTable clone() {
        LinearProbingHashTable clone = new LinearProbingHashTable(
                this.valueBuffer.length,
                this.valSz,
                this.name
        );

        final int length = this.table.length;
        byte[] inner = new byte[length];
        System.arraycopy(this.table, 0, inner, 0, length);
        clone.table = inner;
        clone.slots = this.slots;
        clone.sz = this.sz;
        clone.threshold = this.threshold;
        return clone;
    }

    public byte[] get(long key) {
        int slot = getSlot(key);
        ByteArrays.longToArray(key, keyBuffer);

        while (!isEmpty(slot)) {
            if (keyEquals(slot, keyBuffer)) {
                byte[] val = new byte[valSz];
                readVal(slot, table, val);
                return val;
            }
            slot = nextSlot(slot);
        }

        return null;
    }

    public byte[] get(long key, byte[] valBuffer) {
        int slot = getSlot(key);
        ByteArrays.longToArray(key, keyBuffer);

        while (!isEmpty(slot)) {
            if (keyEquals(slot, keyBuffer)) {
                readVal(slot, table, valBuffer);
                return valBuffer;
            }
            slot = nextSlot(slot);
        }

        return null;
    }

    public byte[] get2(long key, byte[] buffer) {
        int slot = getSlot(key);
        ByteArrays.longToArray(key, keyBuffer);

        while (!isEmpty(slot)) {
            if (keyEquals(slot, keyBuffer)) {
                return getValAndSlotIndex(slot, table, buffer);
            }
            slot = nextSlot(slot);
        }

        return null;
    }

    public void readValue(final int slotIndex, byte[] valueBuffer) {
        readVal(slotIndex, table, valueBuffer);
    }

    public void readEntry(final int slotIndex, byte[] buffer) {
        readEntry(slotIndex, table, buffer);
    }

    private int nextSlot(int slot) {
        slot++;
        if (slot * slotSz >= table.length) {
            slot = 0;
        }
        return slot;
    }

    public int size() {
        return sz;
    }

    public int length() {
        return table.length;
    }

    /**
     * @return how many slots were scanned before empty. If the value is large, consider expanding
     * the table
     */
    public int set(long key, byte[] val) {
        int slot = getSlot(key);
        int scanLength = 0;
        ByteArrays.longToArray(key, keyBuffer);

        boolean empty = isEmpty(slot) || isReset(slot);
        while (!empty && !keyEquals(slot, keyBuffer)) {
            slot = nextSlot(slot);
            scanLength++;
            empty = isEmpty(slot) || isReset(slot);
        }

        if (empty) {
            sz++;
        }
        int offset = slot * slotSz;

        System.arraycopy(keyBuffer, 0, table, offset, KEY_SZ);
        offset += KEY_SZ;
        System.arraycopy(val, 0, table, offset, valSz);

        if (sz > threshold) {
            long start = System.nanoTime();
            int oldLength = table.length;
            resize();
            long end = System.nanoTime();
            logger.info("Resized {} from {} to {} in {} ns", name, oldLength, table.length, (end - start));
        }
        return scanLength;
    }
    
    /**
     * More efficient set using the int.
     * @param key The key to set.
     * @param val The value to set.
     * @return how many slots were scanned before empty. If the value is large, consider expanding
     * the table
     */
    public int set(long key, int val) {
      int slot = getSlot(key);
      int scanLength = 0;
      ByteArrays.longToArray(key, keyBuffer);

      boolean empty = isEmpty(slot) || isReset(slot);
      while (!empty && !keyEquals(slot, keyBuffer)) {
          slot = nextSlot(slot);
          scanLength++;
          empty = isEmpty(slot) || isReset(slot);
      }

      if (empty) {
          sz++;
      }
      int offset = slot * slotSz;

      System.arraycopy(keyBuffer, 0, table, offset, KEY_SZ);
      offset += KEY_SZ;
      table[offset++] = (byte) (val >> 24);
      table[offset++] = (byte) (val >> 16);
      table[offset++] = (byte) (val >> 8);
      table[offset++] = (byte) val;

      if (sz > threshold) {
          long start = System.nanoTime();
          int oldLength = table.length;
          resize();
          long end = System.nanoTime();
          logger.info("Resized {} from {} to {} in {} ns", name, oldLength, table.length, (end - start));
      }
      return scanLength;
  }

    private void resize() {

        byte[] oldTable = table;
        int oldCap = slots;

        int newCap = oldCap;
        if (oldTable != null) { // growing the table;
            newCap = oldCap << 1; // double the size;
        }

        BigInteger p = BigInteger.valueOf(newCap);
        newCap = p.nextProbablePrime().intValueExact();

        int newThr = 0;
        if (newCap < MAXIMUM_CAPACITY) {
            newThr = (int) (newCap * DEFAULT_LOAD_FACTOR);
        } else {
            newThr = Integer.MAX_VALUE;
        }

        table = new byte[newCap * slotSz];
        sz = 0;
        slots = newCap;
        threshold = newThr;
        if (oldTable != null) {
            for (int i = 0; i < oldCap; i++) {
                long key = getKey(i, oldTable);
                if (key != 0 && key != -1) {
                    readVal(i, oldTable, valueBuffer);
                    set(key, valueBuffer);
                }
            }
        }
    }

    private void readEntry(int slot, byte[] table, byte[] buffer) {
        int offset = slot * slotSz;
        System.arraycopy(table, offset, buffer, 0, slotSz);
    }

    private void readVal(int slot, byte[] table, byte[] val) {
        int offset = slot * slotSz + KEY_SZ;
        System.arraycopy(table, offset, val, 0, valSz);
    }

    private byte[] getValAndSlotIndex(int slot, byte[] table) {
        byte[] val = new byte[valSz + 4];
        return getValAndSlotIndex(slot, table, val);
    }

    private byte[] getValAndSlotIndex(int slot, byte[] table, byte[] buffer) {
        int offset = slot * slotSz + KEY_SZ;
        System.arraycopy(table, offset, buffer, 0, valSz);
        ByteArrays.putInt(slot, buffer, valSz);
        return buffer;
    }

    private long getKey(int slot, byte[] table) {
        int offset = slot * slotSz;
        return ByteArrays.getLong(table, offset);
    }

    private boolean keyEquals(int slot, byte[] key) {
        int offset = slot * slotSz;
        for (int i = 0; i < KEY_SZ; i++) {
            if (table[offset + i] != key[i]) {
                return false;
            }
        }
        return true;
    }

    private boolean isEmpty(int slot) {
        int offset = slot * slotSz;
        for (int i = 0; i < KEY_SZ; i++) {
            if (table[offset + i] != 0) {
                return false;
            }
        }
        return true;
    }

    private boolean isReset(int slot) {
        int offset = slot * slotSz;
        for (int i = 0; i < KEY_SZ; i++) {
            if (table[offset + i] != -1) {
                return false;
            }
        }
        return true;
    }

    private int getSlot(long key) {
        // a bit slow, another option is to use a size that is multiplier of 2 and bitmask it
        return Math.abs((int) (key % slots));
    }

    public long[] keys() {
        long[] keys = new long[sz];
        int i = 0;
        for (int slot = 0; slot < table.length / slotSz; slot++) {
            if (!isEmpty(slot) && !isReset(slot)) {
                keys[i++] = getKey(slot, table);
            }
        }
        return keys;
    }

    public byte[][] values() {
        byte[][] values = new byte[sz][];
        int i = 0;
        for (int slot = 0; slot < table.length / slotSz; slot++) {
            if (!isEmpty(slot) && !isReset(slot)) {
                byte[] val = new byte[valSz];
                readVal(slot, table, val);
                values[i++] = val;
            }
        }
        return values;
    }

    public void readSlotIndex(final int[] result) throws InSufficientBufferLengthException {
        if(result.length < sz) {
            throw new InSufficientBufferLengthException(sz, result.length);
        }
        int i = 0;
        for (int slot = 0; slot < table.length / slotSz; slot++) {
            if (!isEmpty(slot) && !isReset(slot)) {
                result[i++] = slot;
            }
        }
    }

    /**
     * @return byte array containing addr, len and slot index. It gives direct access to the slot
     * for better performance.
     */
    public byte[][] metaEntries() {
        byte[][] entries = new byte[sz][];
        int i = 0;
        for (int slot = 0; slot < table.length / slotSz; slot++) {
            if (!isEmpty(slot) && !isReset(slot)) {
                entries[i++] = getValAndSlotIndex(slot, table);
            }
        }
        return entries;
    }

    public byte[][] entries() {
        byte[][] entries = new byte[sz][];
        int i = 0;
        for (int slot = 0; slot < table.length / slotSz; slot++) {
            if (!isEmpty(slot) && !isReset(slot)) {
                byte[] entry = new byte[slotSz + 4];
                int offset = slot * slotSz;
                System.arraycopy(table, offset, entry, 0, slotSz);
                ByteArrays.putInt(slot, entry, slotSz);
                entries[i++] = entry;
            }
        }
        return entries;
    }

    public boolean resetSlot(int slot) {
        if (!isEmpty(slot) && !isReset(slot)) {
            int start = slot * slotSz;
            int end = start + slotSz;
            for (int i = start; i < end; i++) {
                table[i] = -1;
            }
            sz--;
            return true;
        }
        return false;
    }

}
