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

import net.opentsdb.aura.metrics.core.data.ResultantPointerArray;
import net.opentsdb.aura.metrics.pools.AMTSPoolAllocator;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.pools.ObjectPool;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class AuraMetricsTimeSeriesList implements List<TimeSeries>, Closeable {

  protected final AuraMetricsQueryResult result;
  protected final ResultantPointerArray[] pointers;
  protected final int[] counts;
  private int firstSegmentTime;
  private int segmentCount;
  protected final int total;
  protected ObjectPool pool;

  public AuraMetricsTimeSeriesList(
      final AuraMetricsQueryResult result, final long[] pointers, final int firstSegmentTime, final int segmentCount) {
    this.result = result;
    this.pointers = new ResultantPointerArray[pointers.length];
    this.counts = new int[pointers.length];
    this.firstSegmentTime = firstSegmentTime;
    this.segmentCount = segmentCount;
    int total = 0;
    for (int i = 0; i < pointers.length; i++) {
      this.pointers[i] = new ResultantPointerArray(0);
      if (pointers[i] != 0) {
        this.pointers[i].reuse(pointers[i]);
        counts[i] = this.pointers[i].getTSCount();
        total += counts[i];
      }
    }
    this.total = total;
    pool =
        result
            .source()
            .pipelineContext()
            .tsdb()
            .getRegistry()
            .getObjectPool(AMTSPoolAllocator.TYPE);
  }

  @Override
  public void close() {
    for (int i = 0; i < pointers.length; i++) {
      if (pointers[i].getAddress() != 0) {
        pointers[i].free();
      }
    }
  }

  @Override
  public int size() {
    return total;
  }

  @Override
  public boolean isEmpty() {
    return total == 0;
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public Iterator<TimeSeries> iterator() {
    return new LocalIterator();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public boolean add(TimeSeries timeSeries) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public boolean addAll(Collection<? extends TimeSeries> c) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public boolean addAll(int index, Collection<? extends TimeSeries> c) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public TimeSeries get(final int index) {
    int pointer_idx = 0;
    int count = 0;
    for (int i = 0; i < counts.length; i++) {
      count += counts[i];
      if (count >= (index + 1)) {
        break;
      }
      pointer_idx++;
    }

    if (pointer_idx >= pointers.length) {
      throw new IndexOutOfBoundsException(String.valueOf(pointer_idx));
    }

    ResultantPointerArray pointerArray = pointers[pointer_idx];
    int ptr_offset = count - counts[pointer_idx];
    ptr_offset = index - ptr_offset;

    final long tsAddress = pointerArray.getTSAddress(ptr_offset);
    final long tagAddress = pointerArray.getTagAddress(ptr_offset);
    final int tagLength = pointerArray.getTagLength(ptr_offset);
    final AuraMetricsTimeSeries ts;
    if (pool != null) {
      ts = (AuraMetricsTimeSeries) pool.claim().object();
    } else {
      ts = new AuraMetricsTimeSeries();
    }
    ts.reset(result.source(), result, tsAddress, tagAddress, tagLength, firstSegmentTime, segmentCount);
    return ts;
  }

  @Override
  public TimeSeries set(int index, TimeSeries element) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void add(int index, TimeSeries element) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public TimeSeries remove(int index) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public int indexOf(Object o) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public ListIterator<TimeSeries> listIterator() {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public ListIterator<TimeSeries> listIterator(int index) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public List<TimeSeries> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  class LocalIterator implements Iterator<TimeSeries> {
    int readIdx;

    @Override
    public boolean hasNext() {
      return readIdx < total;
    }

    @Override
    public TimeSeries next() {
      return get(readIdx++);
    }
  }
}
