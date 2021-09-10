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
package net.opentsdb.aura.metrics.system;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestRhel7MemoryInfoReader {

  private static final String filePath = TestRhel7MemoryInfoReader.class.getClassLoader()
      .getResource("proc/meminfo.el7").getFile();
  private static final long totalExpected = 65448940;
  private static final long freeExpected = 51510724;

  private Rhel7MemoryInfoReader memInfoReader;

  //@Test
  public void testTotalMemory() {
    memInfoReader = new Rhel7MemoryInfoReader(1000, filePath);

    assertEquals(memInfoReader.getTotalPhysicalMemory(), totalExpected);
    assertEquals(memInfoReader.getFreePhysicalMemory(), freeExpected);
    assertEquals(memInfoReader.getTotalSwapMemory(), 65011708);
    assertEquals(memInfoReader.getFreeSwapMemory(), 64631028);
  }

  //@Test
  public void testGetTotalMemoryUsage() {
    memInfoReader = new Rhel7MemoryInfoReader(1000, filePath);

    double expected = ((double) totalExpected - freeExpected) / (double) totalExpected * 100;
    assertEquals(memInfoReader.getTotalMemoryUsage(), expected);
  }

  //@Test
  public void testInvalidFilePath() {
    String invalidFilePath = "fileNotPresent";
    try {
      memInfoReader = new Rhel7MemoryInfoReader(1000, invalidFilePath);
      fail("Invalid file path");
    } catch (AssertionError expected) {
      assertEquals("File not found: " + invalidFilePath, expected.getMessage());
    }
  }

  //@Test
  public void testScheduledRead() throws InterruptedException {
    // The read() method is called synchronously once, then scheduled. We want
    // to be called at least twice.
    final CountDownLatch latch = new CountDownLatch(2);

    memInfoReader = new Rhel7MemoryInfoReader(1, filePath) {
      @Override
      void read() {
        latch.countDown();
      }
    };

    // Allow a generous deadline.
    final boolean reachedZero = latch.await(3, TimeUnit.MILLISECONDS);
    assertTrue(reachedZero);
  }
}
