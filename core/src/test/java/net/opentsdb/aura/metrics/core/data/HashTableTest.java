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

import com.google.common.collect.ImmutableList;
import net.opentsdb.aura.metrics.core.XxHash;
import net.opentsdb.hashing.HashFunction;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HashTableTest {

  @Test
  public void testCollisions() {
    HashTable table = new HashTable("AppTable", 5);
    table.put(1, "1".getBytes());
    table.put(2, "2".getBytes());
    table.put(3, "3".getBytes());
    table.put(4, "4".getBytes());
    table.put(5, "5".getBytes());
    assertEquals("1", new String(table.get(1)));
    assertEquals("2", new String(table.get(2)));
    assertEquals("3", new String(table.get(3)));
    assertEquals("4", new String(table.get(4)));
    assertEquals("5", new String(table.get(5)));
    table.put(1, "11".getBytes());
    table.put(2, "12".getBytes());
    table.put(3, "13".getBytes());
    table.put(4, "14".getBytes());
    table.put(5, "15".getBytes());
    assertEquals("11", new String(table.get(1)));
    assertEquals("12", new String(table.get(2)));
    assertEquals("13", new String(table.get(3)));
    assertEquals("14", new String(table.get(4)));
    assertEquals("15", new String(table.get(5)));
  }

  @Test
  public void testGrowableHashTable() {

    HashTable hashTable = new HashTable("AppTable", 2);
    hashTable.put(1, "1".getBytes());
    hashTable.put(2, "2".getBytes());
    hashTable.put(3, "3".getBytes());
    hashTable.put(3, "4".getBytes());
    hashTable.put(5, "5".getBytes());

    assertEquals(new String(hashTable.get(1)), "1");
    assertEquals(new String(hashTable.get(2)), "2");
    assertEquals(new String(hashTable.get(3)), "4");
    assertNull(hashTable.get(4));
    assertEquals(new String(hashTable.get(5)), "5");
  }

  @Test
  public void testMetaEntries() {

    HashTable hashTable = new HashTable("AppTable", 2);
    hashTable.put(1, "1".getBytes());
    hashTable.put(2, "2".getBytes());
    hashTable.put(3, "3".getBytes());
    hashTable.put(4, "4".getBytes());

    byte[][] metaEntries = hashTable.metaEntries();
    assertEquals(metaEntries.length, 4);

    List<Integer> values = new ArrayList<>();
    List<Integer> slotIndexes = new ArrayList<>();

    Arrays.stream(metaEntries)
        .forEach(
            (metaEntry) -> {
              long addr = ByteArrays.getLong(metaEntry, 0);
              int len = ByteArrays.getInt(metaEntry, 8);
              byte[] data = new byte[len];
              Memory.read(addr, data, len);
              values.add(Integer.parseInt(new String(data)));
              slotIndexes.add(ByteArrays.getInt(metaEntry, 12));
            });

    assertEquals(ImmutableList.of(1, 2, 3, 4), values);
    assertEquals(ImmutableList.of(1, 2, 3, 4), slotIndexes);
  }

  @Test
  public void testResetSlot() {
    HashTable hashTable = new HashTable("AppTable", 2);
    hashTable.put(1, "1".getBytes());
    hashTable.put(2, "2".getBytes());
    hashTable.put(3, "3".getBytes());
    hashTable.put(4, "4".getBytes());

    assertEquals(hashTable.size(), 4);

    byte[][] metaEntries = hashTable.metaEntries();
    assertEquals(metaEntries.length, 4);

    int slotOffset = ByteArrays.getInt(metaEntries[1], 12);
    hashTable.resetSlot(slotOffset);

    assertEquals(hashTable.size(), 3);
    assertNull(hashTable.get(2));
    assertEquals(new String(hashTable.get(1)), "1");
    assertEquals(new String(hashTable.get(3)), "3");
    assertEquals(new String(hashTable.get(4)), "4");

    hashTable.resetSlot(slotOffset);
    assertEquals(hashTable.size(), 3); // size shouldn't change if reset again.

    metaEntries = hashTable.metaEntries();
    assertEquals(metaEntries.length, 3);
  }

  @Test
  public void testReset2() {
    HashTable hashTable = new HashTable("AppTable", 2);
    hashTable.put(1, "1".getBytes());
    hashTable.put(2, "2".getBytes());
    hashTable.put(3, "3".getBytes());
    hashTable.put(4, "4".getBytes());

    byte[] pointerBuffer = new byte[HashTable.valSz + 4];

    boolean found = hashTable.getPointer2(3, pointerBuffer);
    assertTrue(found);
    long address = ByteArrays.getLong(pointerBuffer, 0);
    int length = ByteArrays.getInt(pointerBuffer, 8);
    int slot = ByteArrays.getInt(pointerBuffer, 12);

    assertTrue(address > 0);
    assertTrue(length > 0);
    hashTable.resetSlot(slot);

    assertFalse(hashTable.containsKey(3));
    assertTrue(hashTable.containsKey(2));
    assertEquals(3, hashTable.size());

    hashTable.put(3, "33".getBytes());
    assertTrue(hashTable.containsKey(3));
    assertEquals(4, hashTable.size());
  }

  @Test
  public void testReuseDeletedSlot() {
    HashTable hashTable = new HashTable("AppTable", 2);
    hashTable.put(1, "1".getBytes());
    hashTable.put(2, "2".getBytes());

    int initialLength = hashTable.length();

    byte[][] metaEntries = hashTable.metaEntries();
    hashTable.resetSlot(ByteArrays.getInt(metaEntries[1], 12)); // remove 2

    hashTable.put(3, "3".getBytes());

    assertEquals(hashTable.length(), initialLength);
    assertEquals(hashTable.size(), 2);
  }

  @Disabled
  @Test
  public void testJavaHashTable() throws InterruptedException {

    int size = 10;
    //        int size = 10_000_000;

    String app = "system";

    HashFunction hashFunction = new XxHash();

    Map<byte[], byte[]> map = new HashMap(size);
    HashTable table = new HashTable("AppTable", size);
    List<Long> keys = new ArrayList<>(size);

    for (int i = 0; i < 10; i++) {
      byte[] value = (app + i).getBytes();

      long key = hashFunction.hash(value);

      keys.add(key);

      //            map.put(key, value);
      map.put(ByteArrays.longToArray(key), value);

      table.put(key, value);
    }

    for (int i = 0; i < 10; i++) {
      Long key = keys.get(5);
      long start = System.nanoTime();
      map.get(key);
      long end = System.nanoTime();
      System.out.println(i + " " + (end - start) + " ns");
      Thread.sleep(1000);
    }

    System.out.println("****************");

    for (int i = 0; i < 10; i++) {
      Long key = keys.get(5);
      long start = System.nanoTime();
      map.get(key);
      long end = System.nanoTime();
      System.out.println(i + " " + (end - start) + " ns");
      Thread.sleep(1000);
    }
  }
}
