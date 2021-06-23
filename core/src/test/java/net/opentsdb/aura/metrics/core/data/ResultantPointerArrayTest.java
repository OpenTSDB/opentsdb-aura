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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ResultantPointerArrayTest {

  private ResultantPointerArray array;

  @Test
  public void testInitializeByCapacity() {
    array = new ResultantPointerArray(2);
    Random random = new Random();

    long tsAddress1 = random.nextLong();
    long tagAddress1 = random.nextLong();
    int tagLength1 = random.nextInt();

    long tsAddress2 = random.nextLong();
    long tagAddress2 = random.nextLong();
    int tagLength2 = random.nextInt();

    array.set(0, tsAddress1, tagAddress1, tagLength1);
    array.set(1, tsAddress2, tagAddress2, tagLength2);
    array.setTSCount(2);

    assertEquals(tsAddress1, array.getTSAddress(0));
    assertEquals(tagAddress1, array.getTagAddress(0));
    assertEquals(tagLength1, array.getTagLength(0));

    assertEquals(tsAddress2, array.getTSAddress(1));
    assertEquals(tagAddress2, array.getTagAddress(1));
    assertEquals(tagLength2, array.getTagLength(1));

    assertEquals(2, array.getTSCount());

    assertEquals(2, array.getCapacity());
    assertTrue(array.getAddress() > 0);
  }

  @Test
  public void testInitializeByStartAddress() {
    array = new ResultantPointerArray(3);
    array.set(0, 123, 345, 12);
    array.set(1, 123, 345, 12);
    array.set(2, 123, 345, 12);

    array.setTSCount(3);

    long startAddress = array.getAddress();

    ResultantPointerArray newArray = new ResultantPointerArray(startAddress);
    assertEquals(startAddress, newArray.getAddress());
    assertEquals(array.getCapacity(), newArray.getCapacity());
    assertEquals(3, newArray.getTSCount());
  }

  @Test
  public void testEmptyArray() {
    array = new ResultantPointerArray(0);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
    assertEquals(0, array.getTSCount());

    try {
      array.getTSAddress(0);
      fail("Should not read from a invalid index");
    } catch (IndexOutOfBoundsException expected) {
      assertEquals(String.valueOf(0), expected.getMessage());
    }

    try {
      array.getTagAddress(0);
      fail("Should not read from a invalid index");
    } catch (IndexOutOfBoundsException expected) {
      assertEquals(String.valueOf(0), expected.getMessage());
    }

    try {
      array.getTagLength(0);
      fail("Should not read from a invalid index");
    } catch (IndexOutOfBoundsException expected) {
      assertEquals(String.valueOf(0), expected.getMessage());
    }
  }

  @Test
  public void testWhenArrayIsPartiallyFilled() {
    array = new ResultantPointerArray(5);
    Random random = new Random();

    long tsAddress1 = random.nextLong();
    long tagAddress1 = random.nextLong();
    int tagLength1 = random.nextInt();

    long tsAddress2 = random.nextLong();
    long tagAddress2 = random.nextLong();
    int tagLength2 = random.nextInt();

    array.set(0, tsAddress1, tagAddress1, tagLength1);
    array.set(1, tsAddress2, tagAddress2, tagLength2);
    array.setTSCount(2);

    assertEquals(tsAddress1, array.getTSAddress(0));
    assertEquals(tagAddress1, array.getTagAddress(0));
    assertEquals(tagLength1, array.getTagLength(0));

    assertEquals(tsAddress2, array.getTSAddress(1));
    assertEquals(tagAddress2, array.getTagAddress(1));
    assertEquals(tagLength2, array.getTagLength(1));

    assertEquals(2, array.getTSCount());

    assertEquals(5, array.getCapacity());
    assertTrue(array.getAddress() > 0);
  }

  @Test
  void testBoundaryCheck() {
    array = new ResultantPointerArray(2);

    try {
      array.set(2, 123, 345, 567);
      fail("Should not write to a out of bound index");
    } catch (IndexOutOfBoundsException expected) {
      assertEquals(expected.getMessage(), String.valueOf(2));
    }

    try {
      array.set(-10, 123, 345, 567);
      fail("Should not write from a out of bound index");
    } catch (IndexOutOfBoundsException expected) {
      assertEquals(expected.getMessage(), String.valueOf(-10));
    }

    try {
      array.getTSAddress(3);
      fail("Should not read from a out of bound index");
    } catch (IndexOutOfBoundsException expected) {
      assertEquals(expected.getMessage(), String.valueOf(3));
    }

    try {
      array.getTagLength(-1);
      fail("Should not read from a out of bound index");
    } catch (IndexOutOfBoundsException expected) {
      assertEquals(expected.getMessage(), String.valueOf(-1));
    }
  }

  @Test
  void testReuseByCapacity() {
    array = new ResultantPointerArray(2);
    long address = array.getAddress();

    long oldAddress = array.create(5);
    assertEquals(address, oldAddress);
    assertEquals(5, array.getCapacity());
    assertTrue(array.getAddress() > 0);
  }

  @Test
  void testReuseByZeroCapacity() {
    array = new ResultantPointerArray(2);
    long address = array.getAddress();
    long oldAddress = array.create(0);
    assertEquals(address, oldAddress);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
    assertEquals(0, array.getTSCount());
  }

  @Test
  void testReuseByStartAddress() {
    ResultantPointerArray another = new ResultantPointerArray(3);
    ResultantPointerArray array = new ResultantPointerArray(2);
    long address = array.getAddress();

    long oldAddress = array.reuse(another.getAddress());

    assertEquals(address, oldAddress);
    assertEquals(3, array.getCapacity());
    assertEquals(another.getAddress(), array.getAddress());
  }

  @Test
  void testReuseByStartAddressZero() {
    ResultantPointerArray array = new ResultantPointerArray(2);
    long address = array.getAddress();

    long oldAddress = array.reuse(0l);

    assertEquals(address, oldAddress);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
    assertEquals(0, array.getTSCount());
  }

  @Test
  void testFree() {
    ResultantPointerArray array = new ResultantPointerArray(2);
    array.free();
    assertEquals(0, array.getAddress());
    assertEquals(0, array.getCapacity());
  }

  @Test
  @Disabled
  public void perfTest() {
    int n = 10_000_000;
    array = new ResultantPointerArray(n);
    Random random = new Random();
    for (int i = 0; i < n; i++) {
      array.set(i, random.nextLong(), random.nextLong(), random.nextInt());
    }

    int itr = 20;
    long sum = 0;
    for (int i = 0; i < itr; i++) {
      long start = System.nanoTime();
      for (int index = 0; index < n; index++) {
        long tsAddress = array.getTSAddress(index);
        long tagAddress = array.getTagAddress(index);
        int tagLength = array.getTagLength(index);
        sum += tsAddress + tagAddress + tagLength;
      }
      long end = System.nanoTime();
      System.out.println("Time to iterate " + n + " entries " + (end - start) + " ns");
      System.out.println("==========" + sum + "===============");
    }
  }
}
