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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GarbageQueueTest {

  @Test
  public void testEmptyQueue() {
    String name = "test";
    GarbageQueue q = new GarbageQueue(name, 10);
    assertEmpty(q, 10, 0, 0);
    assertEquals(q.getName(), name);
  }

  @Test
  public void testAdd() {
    GarbageQueue q = new GarbageQueue("test", 10);
    q.add(100l, 300l);
    q.add(101l, 301l);
    q.add(102l, 302l);

    assertEquals(q.size(), 3);
    assertEquals(q.capacity(), 10);
    assertEquals(q.getFirstIndex(), 0);
    assertEquals(q.getLastIndex(), 3);
    assertHead(q, 100l, 300l);
  }

  @Test
  public void testRemove() {
    GarbageQueue q = new GarbageQueue("test", 10);
    q.add(100, 300);
    q.add(101, 301);
    q.add(102, 302);
    q.add(103, 303);
    q.add(104, 304);
    q.add(105, 305);

    q.remove();

    assertEquals(q.size(), 5);
    assertEquals(q.capacity(), 10);
    assertEquals(q.getFirstIndex(), 1);
    assertEquals(q.getLastIndex(), 6);
    assertHead(q, 101, 301);

    q.remove();
    q.remove();
    q.remove();

    assertEquals(q.size(), 2);
    assertEquals(q.capacity(), 10);
    assertEquals(q.getFirstIndex(), 4);
    assertEquals(q.getLastIndex(), 6);
    assertHead(q, 104, 304);

    q.remove();
    q.remove();
    assertEmpty(q, 10, 6, 6);
  }

  @Test
  public void testRemoveAndAdd() {
    GarbageQueue q = new GarbageQueue("test", 10);
    q.add(100, 300);
    q.add(101, 301);
    q.add(102, 302);
    q.add(103, 303);
    q.add(104, 304);
    q.add(105, 305);

    q.remove();
    q.remove();
    q.remove();

    q.add(106, 306);
    q.add(107, 307);

    assertEquals(q.size(), 5);
    assertEquals(q.capacity(), 10);
    assertEquals(q.getFirstIndex(), 3);
    assertEquals(q.getLastIndex(), 8);
    assertHead(q, 103, 303);
  }

  @Test
  public void testQueueWrapsAround() {
    GarbageQueue q = new GarbageQueue("test", 5);
    q.add(100, 300);
    q.add(101, 301);
    q.add(102, 302);
    q.add(103, 303);

    q.remove();
    q.remove();
    q.remove();

    q.add(104, 304);
    q.add(105, 305);
    q.add(106, 306);

    assertEquals(q.size(), 4);
    assertEquals(q.capacity(), 5);
    assertEquals(q.getFirstIndex(), 3);
    assertEquals(q.getLastIndex(), 2);
    assertHead(q, 103, 303);
  }

  @Test
  public void testFullQ() {
    GarbageQueue q = new GarbageQueue("test", 5);
    q.add(100, 300);
    q.add(101, 301);
    q.add(102, 302);
    q.add(103, 303);
    q.add(104, 304);

    assertEquals(q.size(), 5);
    assertEquals(q.capacity(), 5);
    assertEquals(q.getFirstIndex(), 0);
    assertEquals(q.getLastIndex(), 0);
    assertHead(q, 100, 300);
  }

  @Test
  public void testGrowWhenQueueIsFullAndFirstIsAtIndexZero() {
    int initialCapacity = 5;
    GarbageQueue q = new GarbageQueue("test", initialCapacity);
    q.add(100, 300);
    q.add(101, 301);
    q.add(102, 302);
    q.add(103, 303);
    q.add(104, 304);

    // this addition would grow the queue
    q.add(105, 305);

    assertEquals(q.size(), 6);
    assertEquals(q.capacity(), initialCapacity * 2);
    assertEquals(q.getFirstIndex(), 0);
    assertEquals(q.getLastIndex(), 6);

    assertHead(q, 100, 300);
    q.remove();
    assertHead(q, 101, 301);
    q.remove();
    assertHead(q, 102, 302);
    q.remove();
    assertHead(q, 103, 303);
    q.remove();
    assertHead(q, 104, 304);
    q.remove();
    assertHead(q, 105, 305);
  }

  @Test
  public void testGrowWhenQueueIsFullAndWrappedAround() {
    int initialCapacity = 5;
    GarbageQueue q = new GarbageQueue("test", initialCapacity);
    q.add(100, 300);
    q.add(101, 301);
    q.add(102, 302);
    q.add(103, 303);

    q.remove();
    q.remove();
    q.remove();

    q.add(104, 304);
    q.add(105, 305);
    q.add(106, 306);
    q.add(107, 307);

    assertEquals(q.size(), 5); // q is full
    // the queue is wrapped around, the head of the queue is in between the array
    assertEquals(q.getFirstIndex(), 3);
    assertEquals(q.getLastIndex(), 3);

    // this addition would grow the queue
    q.add(108, 308);

    assertEquals(q.size(), 6);
    assertEquals(q.capacity(), initialCapacity * 2);
    assertEquals(q.getFirstIndex(), 0);
    assertEquals(q.getLastIndex(), 6);

    assertHead(q, 103, 303);
    q.remove();
    assertHead(q, 104, 304);
    q.remove();
    assertHead(q, 105, 305);
    q.remove();
    assertHead(q, 106, 306);
    q.remove();
    assertHead(q, 107, 307);
    q.remove();
    assertHead(q, 108, 308);
  }

  @Test
  public void testGrowWhenQIsFullAndFirstIsAtTheEnd() {
    int initialCapacity = 5;
    GarbageQueue q = new GarbageQueue("test", initialCapacity);
    q.add(100, 300);
    q.add(101, 301);
    q.add(102, 302);
    q.add(103, 303);
    q.add(104, 304);

    q.remove();
    q.remove();
    q.remove();
    q.remove();

    q.add(105, 305);
    q.add(106, 306);
    q.add(107, 307);
    q.add(108, 308);

    assertEquals(q.size(), 5); // q is full
    assertEquals(q.getFirstIndex(), 4); // the head of the queue is at the end of the array
    assertEquals(q.getLastIndex(), 4);

    // this addition would grow the queue
    q.add(109, 309);

    assertEquals(q.size(), 6);
    assertEquals(q.capacity(), initialCapacity * 2);
    assertEquals(q.getFirstIndex(), 0);
    assertEquals(q.getLastIndex(), 6);

    assertHead(q, 104, 304);
    q.remove();
    assertHead(q, 105, 305);
    q.remove();
    assertHead(q, 106, 306);
    q.remove();
    assertHead(q, 107, 307);
    q.remove();
    assertHead(q, 108, 308);
    q.remove();
    assertHead(q, 109, 309);
  }

  @Test
  public void testFillTheQueueAfterGrowing() {
    int initialCapacity = 5;
    GarbageQueue q = new GarbageQueue("test", initialCapacity);
    q.add(100, 300);
    q.add(101, 301);
    q.add(102, 302);
    q.add(103, 303);
    q.add(104, 304);

    q.remove();
    q.remove();
    q.remove();
    q.remove();

    q.add(105, 305);
    q.add(106, 306);
    q.add(107, 307);
    q.add(108, 308);

    assertEquals(q.size(), 5); // q is full

    // next this addition would grow the queue
    q.add(109, 309);

    // fill the queue
    q.add(110, 310);
    q.add(111, 311);
    q.add(112, 312);
    q.add(113, 313);

    assertEquals(q.size(), 10);
    assertEquals(q.capacity(), initialCapacity * 2);
    assertEquals(q.getFirstIndex(), 0);
    assertEquals(q.getLastIndex(), 0);
  }

  private void assertEmpty(
      GarbageQueue q, long capacityExpected, final long firstExpected, final long lastExpected) {
    assertEquals(q.size(), 0);
    assertEquals(q.capacity(), capacityExpected);
    assertEquals(q.getFirstIndex(), firstExpected);
    assertEquals(q.getLastIndex(), lastExpected);
    assertHead(q, 0, 0);
  }

  private void assertHead(GarbageQueue q, final long addressExpected, final long timeExpected) {
    assertEquals(q.peekAddress(), addressExpected);
    assertEquals(q.peekTime(), timeExpected);
  }
}
