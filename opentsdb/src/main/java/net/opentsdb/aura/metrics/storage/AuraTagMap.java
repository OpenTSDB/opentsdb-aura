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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.opentsdb.utils.ByteSet;

/**
 * Aura is storing tags in a concatenated manner by converting the strings to
 * byte arrays where each string is separated by a null character, and dumping 
 * that information off-heap. To avoid creating tons of strings that we don't 
 * need, we can perform operations directly on the byte arrays. But that means
 * we have to implement these interfaces on our own.  
 * 
 * Null values and empty strings are now allowed in the tags data.
 */
class AuraTagMap implements Map<byte[], byte[]> {
  private int size = -1;
  private final int len;
  private final byte[] tag_data;
  
  public AuraTagMap(final int len, final byte[] tag_data) {
    this.len = len > 0 && tag_data[len - 1] == 0 ? len - 1 : len; // off-by-one, don't want a null character.
    this.tag_data = tag_data;
  }
  
  @Override
  public int size() {
    if (size < 0) {
      computeSize();
    }
    return size;
  }

  @Override
  public boolean isEmpty() {
    if (size < 0) {
      computeSize();
    }
    return size == 0;
  }

  @Override
  public boolean containsKey(final Object key) {
    if (key == null || 
        !(key instanceof byte[]) ||
        ((byte[]) key).length < 1) {
      return false;
    }
    
    int idx = 0;
    int count = 0;
    for (int j = 0; j < len; j++) {
      if (tag_data[j] == '\0') {
        if (++count % 2 != 0) {
          if (memcmp((byte[]) key, idx)) {
            return true;
          }
        } else {
          idx = j + 1;
        }
      }
    }
    return false;
  }

  @Override
  public boolean containsValue(final Object value) {
    throw new UnsupportedOperationException("Not implemented yet as we don't "
        + "really have a use yet.");
  }

  @Override
  public byte[] get(final Object key) {
    if (key == null || 
        !(key instanceof byte[]) ||
        ((byte[]) key).length < 1) {
      return null;
    }
    
    // TODO maybe there is a way to perform a binary search? But we'd have to
    // know if a null delimiter was the bound between a key and value or value 
    // and key.
    int idx = 0;
    int count = 0;
    for (int j = 0; j < len; j++) {
      if (tag_data[j] == '\0') {
        if (++count % 2 != 0) {
          if (memcmp((byte[]) key, idx)) {
            j++;
            idx = j;
            // find value
            for (; j < len; j++) {
              if (tag_data[j] == '\0' || j + 1 == len) {
                return Arrays.copyOfRange(tag_data, idx, 
                    j + 1 == len ? j + 1 : j);
              }
            }
                
          }
        } else {
          idx = j + 1;
        }
      }
    }
    return null;
  }

  @Override
  public byte[] put(byte[] key, byte[] value) {
    throw new UnsupportedOperationException("Not implemented yet as we don't "
        + "really have a use yet.");
  }

  @Override
  public byte[] remove(Object key) {
    throw new UnsupportedOperationException("Not implemented yet as we don't "
        + "really have a use yet.");
  }

  @Override
  public void putAll(Map<? extends byte[], ? extends byte[]> m) {
    throw new UnsupportedOperationException("Not implemented yet as we don't "
        + "really have a use yet.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not implemented yet as we don't "
        + "really have a use yet.");
  }

  @Override
  public Set<byte[]> keySet() {
    // blech, wasteful but I'm too lazy to implement a set and we're not really
    // using it.
    final Set<byte[]> set = new ByteSet();
    int idx = 0;
    int count = 0;
    for (int j = 0; j < len; j++) {
      if (tag_data[j] == '\0') {
        if (++count % 2 != 0) {
          set.add(Arrays.copyOfRange(tag_data, idx, j));
        } else {
          idx = j + 1;
        }
      }
    }
    return set;
  }

  @Override
  public Collection<byte[]> values() {
    // blech, wasteful but I'm too lazy to implement a set and we're not really
    // using it.
    final Set<byte[]> set = new ByteSet();
    int idx = 0;
    int count = 0;
    for (int j = 0; j < len; j++) {
      if (tag_data[j] == '\0' || j + 1 == len) {
        if (++count % 2 == 0) {
          set.add(Arrays.copyOfRange(tag_data, idx, 
              j + 1 == len ? j + 1 : j));
        } else {
          idx = j + 1;
        }
      }
    }
    return set;
  }

  @Override
  public Set<Entry<byte[], byte[]>> entrySet() {
    return new EntrySet();
  }
  
  private class EntrySet implements Set<Entry<byte[], byte[]>> {

    @Override
    public int size() {
      return AuraTagMap.this.size();
    }

    @Override
    public boolean isEmpty() {
      return AuraTagMap.this.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
      return new LocalIterator();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public boolean add(Entry<byte[], byte[]> e) {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public boolean addAll(Collection<? extends Entry<byte[], byte[]>> c) {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException("Not implemented yet as we don't "
          + "really have a use yet.");
    }
    
    private class LocalIterator implements Iterator<Entry<byte[], byte[]>> {
      int idx = 0;
      int count = 0;
      
      @Override
      public boolean hasNext() {
        return idx + 1 < len;
      }

      @Override
      public Entry<byte[], byte[]> next() {
        AbstractMap.SimpleEntry<byte[], byte[]> entry = null;
        for (int j = idx; j < len; j++) {
          if (tag_data[j] == '\0') {
            if (++count % 2 != 0) {
              int end_key = j;
              j++;
              // find value
              for (; j < len; j++) {
                if (tag_data[j] == '\0' || j + 1 == len) {
                  entry = new AbstractMap.SimpleEntry<byte[], byte[]>(
                      Arrays.copyOfRange(tag_data, idx, end_key),
                      Arrays.copyOfRange(tag_data, end_key + 1, 
                          j + 1 == len ? j + 1 : j));
                  idx = j;
                  return entry;
                }
              }
            } else {
              idx = j + 1;
            }
          }
        }
        return entry;
      }
      
    }
    
  }
  
  /**
   * Searches the byte array at the given offset to see if the needle matches
   * the value at the offset. Matches the entire length of the needle from 0
   * to needle.length against the offstet in the tag data array.
   * @param needle The term to search for.
   * @param offset An offset to start searching at.
   * @return
   */
  private boolean memcmp(final byte[] needle, int offset) {
    if (len < needle.length) {
      return false;
    }
    
    for (int i = 0; i < needle.length; i++) {
      if (needle[i] != tag_data[offset]) {
        return false;
      }
      offset++;
    }
    if (offset < len && tag_data[offset] != '\0') {
      return false;
    }
    return true;
  }
  
  /**
   * Computes the size and sets it. Used lazily.
   */
  private void computeSize() {
    size = 0;
    int count = 0;
    for (int j = 0; j < len; j++) {
      if (tag_data[j] == '\0') {
        if (++count % 2 == 0) {
          size++;
        }
      }
    }
    // catch end of array case.
    if (count >= 1 && count % 2 != 0) {
      size++;
    }
  }
}
