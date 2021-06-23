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

package net.opentsdb.aura.metrics.core;

import net.opentsdb.aura.metrics.core.data.Memory;
import net.opentsdb.collections.DirectLongArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A single threaded implementation of a garbage queue. */
public class GarbageQueue {

  private static Logger logger = LoggerFactory.getLogger(GarbageQueue.class);

  private static final int SLOT_SIZE_LONG = 2;
  private static final int SLOT_SIZE_BYTES = 2 * Long.BYTES;
  private static final int OFFSET_TIME = 1;

  private String name;
  private DirectLongArray q;
  private int capacity;
  private int head = 0;
  private int tail = 0;
  private int size;

  public GarbageQueue(final String name, final int initialCapacity) {
    this.name = name;
    this.q = new DirectLongArray(initialCapacity * SLOT_SIZE_LONG, false);
    this.capacity = initialCapacity;
  }

  public void add(long address, long time) {
    if (size == capacity) {
      // q is full. Grow it

      long start = System.nanoTime();
      int oldCapacity = capacity;
      int newCapacity = oldCapacity * 2;
      long oldAddress = q.init(newCapacity * SLOT_SIZE_LONG, false); // creates a new array
      DirectLongArray oldQ = new DirectLongArray(oldAddress, false, oldCapacity * SLOT_SIZE_LONG);

      // copy the elements to the new array.
      if (head == 0) {
        Memory.unsafe.copyMemory(
            oldAddress, q.getAddress(), oldCapacity * SLOT_SIZE_LONG * Long.BYTES);
      } else {
        // the queue wraps around.
        Memory.unsafe.copyMemory(
            oldAddress + head * Long.BYTES,
            q.getAddress(),
            (oldCapacity * SLOT_SIZE_LONG - head) * Long.BYTES);
        Memory.unsafe.copyMemory(
            oldAddress,
            q.getAddress() + (oldCapacity * SLOT_SIZE_LONG - head) * Long.BYTES,
            tail * Long.BYTES);
      }

      oldQ.free();

      long end = System.nanoTime();
      logger.info(
          "Resized garbage queue name: {} from {} to {} in {} ns",
          name,
          capacity,
          newCapacity,
          (end - start));

      this.capacity = newCapacity;
      this.head = 0;
      this.tail = size * SLOT_SIZE_LONG;
    }
    q.set(tail, address);
    q.set(tail + OFFSET_TIME, time);
    tail += SLOT_SIZE_LONG;
    if (tail / SLOT_SIZE_LONG >= capacity) {
      tail = 0;
    }
    size++;
  }

  public void remove() {
    if (size > 0) {
      q.set(head, 0);
      q.set(head + OFFSET_TIME, 0);
      head += SLOT_SIZE_LONG;
      if (head / SLOT_SIZE_LONG >= capacity) {
        head = 0;
      }
      size--;
    }
  }

  public long peekAddress() {
    return q.get(head);
  }

  public long peekTime() {
    return q.get(head + OFFSET_TIME);
  }

  public int size() {
    return size;
  }

  public int capacity() {
    return capacity;
  }

  /** @return returns the first logical index */
  public int getFirstIndex() {
    return head / SLOT_SIZE_LONG;
  }

  /** @return returns the last logical index */
  public int getLastIndex() {
    return tail / SLOT_SIZE_LONG;
  }

  public String getName() {
    return name;
  }
}
