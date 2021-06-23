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

package net.opentsdb.aura.metrics.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.testng.annotations.Test;

import net.opentsdb.common.Const;

public class AuraTagMapTest {

  private static final byte[] EMPTY = new byte[0];
  private static final byte[] ONE = buildTags("Key", "Value");
  private static final byte[] MANY = buildTags(
          "Key1", "Value1",
          "Key2", "Value2",
          "Key3", "Value3",
          "Key4", "Value4");

  @Test
  public void testSizeIsEmpty() throws Exception {
    AuraTagMap map = new AuraTagMap(EMPTY.length, EMPTY);
    assertTrue(map.isEmpty());
    assertEquals(map.size(), 0);

    map = new AuraTagMap(ONE.length, ONE);
    assertFalse(map.isEmpty());
    assertEquals(map.size(), 1);

    map = new AuraTagMap(MANY.length, MANY);
    assertFalse(map.isEmpty());
    assertEquals(map.size(), 4);

    byte[] null_at_end = Arrays.copyOf(MANY, MANY.length + 1);
    map = new AuraTagMap(null_at_end.length, null_at_end);
    assertFalse(map.isEmpty());
    assertEquals(map.size(), 4);
  }

  @Test
  public void containsKey() throws Exception {
    AuraTagMap map = new AuraTagMap(EMPTY.length, EMPTY);
    assertFalse(map.containsKey("Key1".getBytes(Const.UTF8_CHARSET)));

    map = new AuraTagMap(ONE.length, ONE);
    assertTrue(map.containsKey("Key".getBytes(Const.UTF8_CHARSET)));
    assertFalse(map.containsKey("Key1".getBytes(Const.UTF8_CHARSET)));
    assertFalse(map.containsKey("Key"));
    assertFalse(map.containsKey(null));
    assertFalse(map.containsKey(new byte[0]));

    map = new AuraTagMap(MANY.length, MANY);
    assertTrue(map.containsKey("Key1".getBytes(Const.UTF8_CHARSET)));
    assertTrue(map.containsKey("Key2".getBytes(Const.UTF8_CHARSET)));
    assertTrue(map.containsKey("Key3".getBytes(Const.UTF8_CHARSET)));
    assertTrue(map.containsKey("Key4".getBytes(Const.UTF8_CHARSET)));
    assertFalse(map.containsKey("Key5".getBytes(Const.UTF8_CHARSET)));
    // don't match a sub string
    assertFalse(map.containsKey("Key".getBytes(Const.UTF8_CHARSET)));
  }

  @Test
  public void get() throws Exception {
    AuraTagMap map = new AuraTagMap(EMPTY.length, EMPTY);
    assertNull(map.get("Key".getBytes(Const.UTF8_CHARSET)));

    map = new AuraTagMap(ONE.length, ONE);
    assertArrayEquals("Value".getBytes(Const.UTF8_CHARSET),
            map.get("Key".getBytes(Const.UTF8_CHARSET)));
    assertNull(map.get("Key1".getBytes(Const.UTF8_CHARSET)));
    assertNull(map.get("Ke".getBytes(Const.UTF8_CHARSET)));
    assertNull(map.get(EMPTY));
    assertNull(map.get("Key"));
    assertNull(map.get(null));

    map = new AuraTagMap(MANY.length, MANY);
    assertArrayEquals("Value1".getBytes(Const.UTF8_CHARSET),
            map.get("Key1".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("Value2".getBytes(Const.UTF8_CHARSET),
            map.get("Key2".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("Value3".getBytes(Const.UTF8_CHARSET),
            map.get("Key3".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("Value4".getBytes(Const.UTF8_CHARSET),
            map.get("Key4".getBytes(Const.UTF8_CHARSET)));
  }

  @Test
  public void keySet() throws Exception {
    AuraTagMap map = new AuraTagMap(EMPTY.length, EMPTY);
    assertTrue(map.keySet().isEmpty());

    map = new AuraTagMap(ONE.length, ONE);
    Set<byte[]> set = map.keySet();
    assertEquals(set.size(), 1);
    // possible due to ByteSet.
    assertTrue(set.contains("Key".getBytes(Const.UTF8_CHARSET)));

    map = new AuraTagMap(MANY.length, MANY);
    set = map.keySet();
    assertEquals(set.size(), 4);
    assertTrue(set.contains("Key1".getBytes(Const.UTF8_CHARSET)));
    assertTrue(set.contains("Key2".getBytes(Const.UTF8_CHARSET)));
    assertTrue(set.contains("Key3".getBytes(Const.UTF8_CHARSET)));
    assertTrue(set.contains("Key4".getBytes(Const.UTF8_CHARSET)));
  }

  @Test
  public void values() throws Exception {
    AuraTagMap map = new AuraTagMap(EMPTY.length, EMPTY);
    assertTrue(map.values().isEmpty());

    map = new AuraTagMap(ONE.length, ONE);
    Collection<byte[]> set = map.values();
    assertEquals(set.size(), 1);
    // possible due to ByteSet.
    assertTrue(set.contains("Value".getBytes(Const.UTF8_CHARSET)));

    map = new AuraTagMap(MANY.length, MANY);
    set = map.values();
    assertEquals(set.size(), 4);
    assertTrue(set.contains("Value1".getBytes(Const.UTF8_CHARSET)));
    assertTrue(set.contains("Value2".getBytes(Const.UTF8_CHARSET)));
    assertTrue(set.contains("Value3".getBytes(Const.UTF8_CHARSET)));
    assertTrue(set.contains("Value4".getBytes(Const.UTF8_CHARSET)));
  }

  @Test
  public void entrySet() throws Exception {
    AuraTagMap map = new AuraTagMap(EMPTY.length, EMPTY);
    Set<Entry<byte[], byte[]>> set = map.entrySet();
    assertTrue(set.isEmpty());

    map = new AuraTagMap(ONE.length, ONE);
    set = map.entrySet();
    assertEquals(1, set.size());
    // ordered in our implementation
    Iterator<Entry<byte[], byte[]>> iterator = set.iterator();
    Entry<byte[], byte[]> entry = iterator.next();
    assertArrayEquals("Key".getBytes(Const.UTF8_CHARSET), entry.getKey());
    assertArrayEquals("Value".getBytes(Const.UTF8_CHARSET), entry.getValue());
    assertFalse(iterator.hasNext());

    map = new AuraTagMap(MANY.length, MANY);
    set = map.entrySet();
    assertEquals(4, set.size());

    iterator = set.iterator();
    entry = iterator.next();
    assertArrayEquals("Key1".getBytes(Const.UTF8_CHARSET), entry.getKey());
    assertArrayEquals("Value1".getBytes(Const.UTF8_CHARSET), entry.getValue());
    assertTrue(iterator.hasNext());

    entry = iterator.next();
    assertArrayEquals("Key2".getBytes(Const.UTF8_CHARSET), entry.getKey());
    assertArrayEquals("Value2".getBytes(Const.UTF8_CHARSET), entry.getValue());
    assertTrue(iterator.hasNext());

    entry = iterator.next();
    assertArrayEquals("Key3".getBytes(Const.UTF8_CHARSET), entry.getKey());
    assertArrayEquals("Value3".getBytes(Const.UTF8_CHARSET), entry.getValue());
    assertTrue(iterator.hasNext());

    entry = iterator.next();
    assertArrayEquals("Key4".getBytes(Const.UTF8_CHARSET), entry.getKey());
    assertArrayEquals("Value4".getBytes(Const.UTF8_CHARSET), entry.getValue());
    assertFalse(iterator.hasNext());
  }

  private static byte[] buildTags(final String... values) {
    int len = 0;
    for (final String string : values) {
      len += string.getBytes(Const.UTF8_CHARSET).length + 1;
    }

    final byte[] tags = new byte[len - 1];
    int i = 0;
    for (final String string : values) {
      final byte[] utf = string.getBytes(Const.UTF8_CHARSET);
      System.arraycopy(utf, 0, tags, i, utf.length);
      i += utf.length;
      i++;
    }
    return tags;
  }
}
