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

import net.opentsdb.collections.DirectByteArray;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffHeapTimeSeriesRecordTest {

  @Test
  public void staticOffsets() {
    assertEquals(0, OffHeapTimeSeriesRecord.METRIC_KEY_INDEX);
    assertEquals(8, OffHeapTimeSeriesRecord.TAG_KEY_INDEX);
    assertEquals(16, OffHeapTimeSeriesRecord.TAG_COUNT_INDEX);
    assertEquals(17, OffHeapTimeSeriesRecord.LAST_TIMESTAMP_INDEX);
    assertEquals(21, OffHeapTimeSeriesRecord.LAST_VALUE_INDEX);
    assertEquals(29, OffHeapTimeSeriesRecord.SEGMENT_ADDR_BASE_INDEX);
  }

  @Test
  public void ctor() {
    int segmentsInATimeSeries = 13; // always + 1
    int secondsInASegment = 3600 * 2;
    int secondsInATimeSeries = secondsInASegment * segmentsInATimeSeries;

    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);

    assertEquals(secondsInASegment, record.secondsInASegment);
    assertEquals(secondsInATimeSeries, record.secondsInATimeSeries);
    assertEquals(29 + (8 * 13), record.recordSizeBytes);
    DirectByteArray dataBlock = record.dataBlock;
    assertNotNull(dataBlock);
    assertEquals(0, dataBlock.getCapacity());
    assertEquals(0, dataBlock.getAddress());

    // < 24h retention
    segmentsInATimeSeries = 7; // always + 1
    record = new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);
    assertEquals(29 + (8 * 7), record.recordSizeBytes);

    // > 24h retention
    segmentsInATimeSeries = 49; // always + 1
    record = new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);
    assertEquals(29 + (8 * 49), record.recordSizeBytes);
  }

  @Test
  public void create() {
    int segmentsInATimeSeries = 13; // always + 1
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);

    int segmentTimestamp = 1620237600;
    int timestamp = 1620239565;
    long addr = record.create(1, 2, (byte) 6, timestamp, 42.5, 1620237600, -1);
    assertTrue(addr > 0);
    assertEquals(1, record.getMetricKey());
    assertEquals(2, record.getTagKey());
    assertEquals(6, record.getTagCount());
    assertEquals(timestamp, record.getLastTimestamp());
    assertEquals(42.5, record.getLastValue(), 0.001);
    assertEquals(29 + (8 * 13), record.length());
    DirectByteArray dataBlock = record.dataBlock;
    assertEquals(addr, dataBlock.getAddress());
    dataBlock.free();

    // NOTE that we don't have any validation here so we can put anything in here.
    addr = record.create(0, 0, (byte) -1, -2, Double.NaN, -2, -1);
    assertTrue(addr > 0);
    assertEquals(0, record.getMetricKey());
    assertEquals(0, record.getTagKey());
    assertEquals(-1, record.getTagCount());
    assertEquals(-2, record.getLastTimestamp());
    assertTrue(Double.isNaN(record.getLastValue()));
    assertEquals(29 + (8 * 13), record.length());
    dataBlock = record.dataBlock;
    assertEquals(addr, dataBlock.getAddress());
    dataBlock.free();

    // try out 0 segments
    segmentsInATimeSeries = 0;
    final OffHeapTimeSeriesRecord badRecord =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);
    assertEquals(29, badRecord.recordSizeBytes);
    assertThrows(
        ArithmeticException.class,
        () -> {
          badRecord.create(1, 2, (byte) 6, timestamp, 42.5, segmentTimestamp, -1);
        });
  }

  @Test
  public void open() {
    int segmentsInATimeSeries = 13; // always + 1
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);

    int segmentTimestamp = 1620237600;
    int timestamp = 1620239565;
    long addr = record.create(1, 2, (byte) 6, timestamp, 42.5, segmentTimestamp, -1);
    assertTrue(addr > 0);

    record = new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);
    record.open(addr);
    assertEquals(1, record.getMetricKey());
    assertEquals(2, record.getTagKey());
    assertEquals(6, record.getTagCount());
    assertEquals(timestamp, record.getLastTimestamp());
    assertEquals(42.5, record.getLastValue(), 0.001);
    DirectByteArray dataBlock = record.dataBlock;
    assertEquals(29 + (8 * 13), dataBlock.getCapacity());
    assertEquals(addr, dataBlock.getAddress());

    // this _should_ segfault. No way to handle it in a UT.
    // record.open(addr * 2);
    // record.getMetricKey();
  }

  @Test
  public void setters() {
    int segmentsInATimeSeries = 13; // always + 1
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);

    int segmentTimestamp = 1620237600;
    int timestamp = 1620239565;
    long addr = record.create(1, 2, (byte) 6, timestamp, 42.5, 1620237600, -1);
    assertTrue(addr > 0);
    assertEquals(1, record.getMetricKey());
    assertEquals(2, record.getTagKey());
    assertEquals(6, record.getTagCount());
    assertEquals(timestamp, record.getLastTimestamp());
    assertEquals(42.5, record.getLastValue(), 0.001);
    assertEquals(29 + (8 * 13), record.length());

    record.setMetricKey(-1);
    record.setTagKey(-2);
    record.setTagCount((byte) 1);
    record.setLastValue(24.8);
    record.setLastTimestamp(timestamp + 60);
    assertEquals(-1, record.getMetricKey());
    assertEquals(-2, record.getTagKey());
    assertEquals(1, record.getTagCount());
    assertEquals(timestamp + 60, record.getLastTimestamp());
    assertEquals(24.8, record.getLastValue(), 0.001);
  }

  static Stream<Arguments> segmentIndexRotation() {
    return Stream.of(
        Arguments.of(13, 3), Arguments.of(2, 1), Arguments.of(3, 0), Arguments.of(25, 8));
  }

  @ParameterizedTest
  @MethodSource("segmentIndexRotation")
  public void segmentIndexRotation(int segmentsInATimeSeries, int startingIndex) {
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);
    assertEquals(29 + (8 * segmentsInATimeSeries), record.recordSizeBytes);

    int segmentTimestamp = 1620237600;
    int ts = segmentTimestamp;
    int expected = startingIndex;
    for (int i = 0; i < 196; i++) {
      int index = record.getSegmentIndex(ts);
      assertEquals(expected, index);
      expected++;
      if (expected > segmentsInATimeSeries - 1) {
        expected = 0;
      }
      ts += secondsInASegment;
    }
  }

  @ParameterizedTest
  @MethodSource("segmentIndexRotation")
  public void segmentSetAndGetAddressByTime(int segmentsInATimeSeries, int startingIndex) {
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);
    assertEquals(29 + (8 * segmentsInATimeSeries), record.recordSizeBytes);
    int segmentTimestamp = 1620237600;
    record.create(1, 2, (byte) 6, 1620237600, 42.5, segmentTimestamp, -1);

    int ts = segmentTimestamp;
    for (int i = 0; i < 196; i++) {
      record.setSegmentAddress(ts, i + 1);
      assertEquals(i + 1, record.getSegmentAddress(ts));
      ts += secondsInASegment;
    }

    record.delete();
  }

  @ParameterizedTest
  @MethodSource("segmentIndexRotation")
  public void segmentSetAndGetAddressByIndex(int segmentsInATimeSeries, int startingIndex) {
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);
    assertEquals(29 + (8 * segmentsInATimeSeries), record.recordSizeBytes);
    int segmentTimestamp = 1620237600;
    record.create(1, 2, (byte) 6, 1620237600, 42.5, segmentTimestamp, -1);

    int ts = segmentTimestamp;
    int expected = startingIndex;
    for (int i = 0; i < 196; i++) {
      record.setSegmentAddressAtIndex(expected, i + 1);
      assertEquals(i + 1, record.getSegmentAddressAtIndex(expected));
      expected++;
      if (expected > segmentsInATimeSeries - 1) {
        expected = 0;
      }
      ts += secondsInASegment;
    }

    record.delete();
  }

  @Test
  public void segmentSetAndGetAddressByIndexOOB() {
    int segmentsInATimeSeries = 13; // always + 1
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);

    int segmentTimestamp = 1620237600;
    int timestamp = 1620239565;
    long addr = record.create(1, 2, (byte) 6, timestamp, 42.5, 1620237600, -1);
    assertTrue(addr > 0);

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          record.setSegmentAddressAtIndex(13, 42);
        });
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          record.setSegmentAddressAtIndex(-1, 42);
        });

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          record.getSegmentAddressAtIndex(13);
        });
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          record.getSegmentAddressAtIndex(-1);
        });
  }

  @Test
  public void getSegmentAddressUninitialized() {
    // in this case we're reading the addresses that haven't been set explicitly.
    // For new records, the addresses are initialized to 0.
    int segmentsInATimeSeries = 13; // always + 1
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);

    int segmentTimestamp = 1620237600;
    int timestamp = 1620239565;
    long addr = record.create(1, 2, (byte) 6, timestamp, 42.5, 1620237600, -1);
    assertTrue(addr > 0);

    for (int i = 0; i < 13; i++) {
      if (i == 3) {
        assertEquals(-1, record.getSegmentAddressAtIndex(i));
      } else {
        assertEquals(0, record.getSegmentAddressAtIndex(i));
      }
    }
  }

  @Test
  public void deleteSegmentAddressAtIndex() {
    int segmentsInATimeSeries = 13; // always + 1
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);

    int segmentTimestamp = 1620237600;
    int timestamp = 1620239565;
    long addr = record.create(1, 2, (byte) 6, timestamp, 42.5, 1620237600, 1024);
    assertTrue(addr > 0);

    for (int i = 0; i < 13; i++) {
      record.setSegmentAddressAtIndex(i, i + 1);
    }

    for (int i = 0; i < 13; i++) {
      if (i % 2 == 0) {
        record.deleteSegmentAddressAtIndex(i);
      }
    }

    for (int i = 0; i < 13; i++) {
      if (i % 2 == 0) {
        assertEquals(0, record.getSegmentAddressAtIndex(i));
      } else {
        assertEquals(i + 1, record.getSegmentAddressAtIndex(i));
      }
    }

    // OOB
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          record.deleteSegmentAddressAtIndex(13);
        });
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          record.deleteSegmentAddressAtIndex(-1);
        });
  }

  @Test
  public void delete() {
    int segmentsInATimeSeries = 13; // always + 1
    int secondsInASegment = 3600 * 2;
    OffHeapTimeSeriesRecord record =
        new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);

    int timestamp = 1620239565;
    long addr = record.create(1, 2, (byte) 6, timestamp, 42.5, 1620237600, 1024);
    assertTrue(addr > 0);
    assertEquals(1024, record.getSegmentAddressAtIndex(3));
    assertEquals(addr, record.getAddress());
    record.delete();

    // read after delete
    // TODO - Need to properly reset in the
    assertEquals(0, record.getAddress());
    assertThrows(IndexOutOfBoundsException.class, () -> record.getSegmentAddressAtIndex(3));
    assertThrows(IndexOutOfBoundsException.class, () -> record.getLastTimestamp());
  }
}
