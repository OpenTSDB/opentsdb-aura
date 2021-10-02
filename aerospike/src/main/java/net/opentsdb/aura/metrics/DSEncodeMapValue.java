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

package net.opentsdb.aura.metrics;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.Value;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.util.Packer;
import net.opentsdb.aura.metrics.core.downsample.AggregationLengthIterator;
import net.opentsdb.aura.metrics.core.downsample.DownSampledTimeSeriesEncoder;
import org.luaj.vm2.LuaValue;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static net.opentsdb.aura.metrics.AerospikeDSTimeSeriesEncoder.encodeMapKey;

public class DSEncodeMapValue implements SortedMap<Value, Value> {

  private DownSampledTimeSeriesEncoder encoder;
  private int recordOffset;
  private int size;

  private EntrySet entrySet;
  private EntrySetIterator entrySetIterator;

  DSEncodeMapValue() {}

  public DSEncodeMapValue(DownSampledTimeSeriesEncoder encoder, int recordOffset) {
    reset(encoder, recordOffset);
  }

  public void reset(DownSampledTimeSeriesEncoder encoder, int recordOffset) {
    this.encoder = encoder;
    this.recordOffset = recordOffset;
    this.size = 1 + encoder.getAggCount(); // +1 for the header entry
  }

  @Override
  public Comparator<? super Value> comparator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<Value, Value> subMap(Value fromKey, Value toKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<Value, Value> headMap(Value toKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<Value, Value> tailMap(Value fromKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value firstKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value lastKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value get(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value put(Value key, Value value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Value remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends Value, ? extends Value> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Value> keySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Value> values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<Value, Value>> entrySet() {
    if (entrySet == null) {
      entrySet = new EntrySet();
    }
    return entrySet;
  }

  private class EntrySet extends AbstractSet<Entry<Value, Value>> {

    @Override
    public Iterator<Entry<Value, Value>> iterator() {
      if (entrySetIterator == null) {
        entrySetIterator = new EntrySetIterator();
      }
      entrySetIterator.reset();
      return entrySetIterator;
    }

    @Override
    public int size() {
      return size;
    }
  }

  private class EntrySetIterator implements Iterator<Entry<Value, Value>> {

    MapEntry entry = new MapEntry();

    @Override
    public boolean hasNext() {
      return entry.index < size - 1;
    }

    @Override
    public Entry<Value, Value> next() {
      entry.index++;
      if (entry.index > 0) {
        entry.iterator.next();
      }
      return entry;
    }

    public void reset() {
      entry.reset();
    }
  }

  private class MapEntry implements Entry<Value, Value> {

    int index;
    AggregationLengthIterator iterator;
    byte[] buffer = ThreadLocalBuffer.get();

    private Value kValue =
        new BaseValue() {
          @Override
          public void pack(Packer packer) {
            if (index == 0) {
              // pack header;
              packer.packByte(encodeMapKey(recordOffset, 0));
            } else {
              packer.packByte(encodeMapKey(recordOffset, iterator.aggOrdinal()));
            }
          }
        };

    private Value vValue =
        new BaseValue() {
          @Override
          public void pack(Packer packer) {
            if (index == 0) {
              // pack header
              int intervalCount = encoder.getIntervalCount();
              int headerLength = (int) (3 + Math.ceil(intervalCount / Byte.SIZE));
              if (buffer.length < headerLength) {
                buffer = ThreadLocalBuffer.resize(headerLength);
              }
              encoder.serializeHeader(buffer, 0);
              packer.packParticleBytes(buffer, 0, headerLength);
            } else {
              // pack aggs
              int aggLength = iterator.aggLengthInBytes();
              if (buffer.length < aggLength) {
                buffer = ThreadLocalBuffer.resize(aggLength);
              }
              iterator.serialize(buffer, 0);
              packer.packParticleBytes(buffer, 0, aggLength);
            }
          }
        };

    @Override
    public Value getKey() {
      return kValue;
    }

    @Override
    public Value getValue() {
      return vValue;
    }

    @Override
    public Value setValue(Value value) {
      throw new UnsupportedOperationException();
    }

    public void reset() {
      index = -1;
      iterator = encoder.aggIterator();
    }
  }

  private abstract class BaseValue extends Value {

    @Override
    public int estimateSize() throws AerospikeException {
      return 0;
    }

    @Override
    public int write(byte[] buffer, int offset) throws AerospikeException {
      return 0;
    }

    @Override
    public int getType() {
      return 0;
    }

    @Override
    public Object getObject() {
      return null;
    }

    @Override
    public LuaValue getLuaValue(LuaInstance instance) {
      return null;
    }
  }

  private static class ThreadLocalBuffer {
    /** Initial buffer size on first use of thread local buffer. */
    public static int DefaultBufferSize = 8192;

    private static final int THREAD_LOCAL_CUTOFF = 1024 * 128; // 128 KB

    private static final ThreadLocal<byte[]> BufferThreadLocal =
        ThreadLocal.withInitial(() -> new byte[DefaultBufferSize]);

    /** Return thread local buffer. */
    public static byte[] get() {
      return BufferThreadLocal.get();
    }

    /**
     * Resize and return thread local buffer if the requested size &lt;= 128 KB. Otherwise, the
     * thread local buffer will not be resized and a new buffer will be returned from heap memory.
     *
     * <p>This method should only be called when the current buffer is too small to hold the desired
     * data.
     */
    public static byte[] resize(int size) {
      if (size > THREAD_LOCAL_CUTOFF) {

        if (Log.debugEnabled()) {
          Log.debug(
              "Thread " + Thread.currentThread().getId() + " allocate buffer on heap " + size);
        }
        return new byte[size];
      }

      if (Log.debugEnabled()) {
        Log.debug("Thread " + Thread.currentThread().getId() + " resize buffer to " + size);
      }
      BufferThreadLocal.set(new byte[size]);
      return BufferThreadLocal.get();
    }
  }
}
