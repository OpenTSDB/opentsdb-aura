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

package net.opentsdb.aura.metrics.core.gorilla;

import io.ultrabrew.metrics.Gauge;
import io.ultrabrew.metrics.MetricRegistry;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NOTE: Tests around reading and writing data are in the
 * GorillaTimeSeriesEncoderTest.java file.
 */
public class OffHeapGorillaSegmentTest {

  private static final int SEGMENT_TIMESTAMP = 1620237600;
  private static final int HOURS_IN_A_DAY = 24;
  private static final int SEGMENT_SIZE_HOUR = 1;
  private static final int SECONDS_IN_A_SEGMENT = (int) TimeUnit.HOURS.toSeconds(SEGMENT_SIZE_HOUR);
  private static final int SEGMENTS_PER_TIMESERIES = HOURS_IN_A_DAY / SEGMENT_SIZE_HOUR + 1;
  private static final int SECONDS_IN_A_TIMESERIES = SECONDS_IN_A_SEGMENT * SEGMENTS_PER_TIMESERIES;

  private long now = System.currentTimeMillis();
  private Random random = new Random(now);

  @Test
  public void ctor(@Mocked MetricRegistry registry,
                   @Mocked Gauge memoryBlockCountGauge,
                   @Mocked Gauge segmentLengthGauge) throws Exception {
    int dataBlockSizeBytes = 256;
    OffHeapGorillaSegment segment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);
    assertEquals(dataBlockSizeBytes, segment.dataBlockSizeBytes);
    assertEquals(dataBlockSizeBytes / 8, segment.dataBlockSizeLongs);
    assertEquals(dataBlockSizeBytes * 8, segment.dataBlockSizeBits);
    assertEquals(0, segment.bitIndex);
    assertFalse(segment.isDirty());
    assertFalse(segment.hasDupesOrOutOfOrderData());
    assertEquals(0, segment.memoryBlockCount);
    assertEquals(0, segment.header.getCapacity());
    assertEquals(0, segment.dataBlock.getCapacity());
    assertEquals(0, segment.getAddress());

    // no address so we're throwing as the capacity is 0.
    assertThrows(IndexOutOfBoundsException.class, () -> {
      segment.getSegmentTime();
    });
    assertThrows(IndexOutOfBoundsException.class, () -> {
      segment.getNumDataPoints();
    });
    assertThrows(IndexOutOfBoundsException.class, () -> {
      segment.getLastTimestamp();
    });
    assertThrows(IndexOutOfBoundsException.class, () -> {
      segment.getLastValue();
    });
    assertThrows(IndexOutOfBoundsException.class, () -> {
      segment.getLastTimestampDelta();
    });
    assertThrows(IndexOutOfBoundsException.class, () -> {
      segment.getLastValueLeadingZeros();
    });
    assertThrows(IndexOutOfBoundsException.class, () -> {
      segment.getLastValueTrailingZeros();
    });

    // just for code coverage
    segment.setTags(new String[0]);
  }

  @Test
  public void createSegment(@Mocked MetricRegistry registry,
                            @Mocked Gauge memoryBlockCountGauge,
                            @Mocked Gauge segmentLengthGauge) throws Exception {
    int dataBlockSizeBytes = 256;
    OffHeapGorillaSegment segment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);
    long addr = segment.createSegment(SEGMENT_TIMESTAMP);
    assertTrue(addr > 0);
    assertEquals(addr, segment.getAddress());
    assertEquals(SEGMENT_TIMESTAMP, segment.getSegmentTime());
    assertEquals(0, segment.getNumDataPoints());
    assertEquals(SEGMENT_TIMESTAMP, segment.getLastTimestamp());
    assertEquals(0, segment.getLastValue());
    assertEquals(0, segment.getLastTimestampDelta());
    assertEquals(0, segment.getLastValueLeadingZeros());
    assertEquals(0, segment.getLastValueTrailingZeros());
    assertEquals(320, segment.bitIndex);
    assertFalse(segment.isDirty());
    assertFalse(segment.hasDupesOrOutOfOrderData());
    assertEquals(1, segment.memoryBlockCount);
    assertEquals(256, segment.header.getCapacity());
    assertEquals(32, segment.dataBlock.getCapacity()); // longs
    assertEquals(segment.header.getAddress(),
            segment.dataBlock.getAddress());
    segment.free();

    // what happens if we set a block size smaller than our header?
    dataBlockSizeBytes = 8;
    final OffHeapGorillaSegment badSegment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);
    assertThrows(IndexOutOfBoundsException.class, () -> {
      badSegment.createSegment(SEGMENT_TIMESTAMP);
    });

    // what if it's too small for the first write?
    dataBlockSizeBytes = 42;
    final OffHeapGorillaSegment badSegment2 = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);
    addr = badSegment2.createSegment(SEGMENT_TIMESTAMP);
    assertTrue(addr > 0);
    assertThrows(IndexOutOfBoundsException.class, () -> {
      badSegment2.writeData(42, Long.BYTES * 8);
    });
  }

  @Test
  public void writeData(@Mocked MetricRegistry registry,
                        @Mocked Gauge memoryBlockCountGauge,
                        @Mocked Gauge segmentLengthGauge) throws Exception {
    int dataBlockSizeBytes = 256;
    final OffHeapGorillaSegment segment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);
    segment.createSegment(SEGMENT_TIMESTAMP);
    assertThrows(IllegalArgumentException.class, () -> {
      segment.writeData(42, -1);
    });
    assertThrows(IllegalArgumentException.class, () -> {
      segment.writeData(42, 65);
    });

    // fill up the header till we hit the next block
    for (int i = 0; i < 26; i++) {
      segment.writeData(0, 64);
      segment.updateHeader();
    }
    assertEquals((dataBlockSizeBytes * 8) - 64, segment.bitIndex);
    assertEquals(1, segment.memoryBlockCount);

    segment.writeData(0, 64);
    segment.updateHeader();
    assertEquals(64, segment.bitIndex);
    assertEquals(2, segment.memoryBlockCount);

    segment.free();

    // one bit left should force a new block
    segment.createSegment(SEGMENT_TIMESTAMP);
    for (int i = 0; i < 26; i++) {
      segment.writeData(0, 64);
      segment.updateHeader();
    }
    assertEquals((dataBlockSizeBytes * 8) - 64, segment.bitIndex);
    assertEquals(1, segment.memoryBlockCount);

    segment.writeData(0, 63);
    segment.updateHeader();
    assertEquals((dataBlockSizeBytes * 8) -1, segment.bitIndex);
    assertEquals(1, segment.memoryBlockCount);

    segment.writeData(0, 1);
    segment.updateHeader();
    assertEquals(64, segment.bitIndex);
    assertEquals(2, segment.memoryBlockCount);
  }

  @Test
  public void readData(@Mocked MetricRegistry registry,
                       @Mocked Gauge memoryBlockCountGauge,
                       @Mocked Gauge segmentLengthGauge) throws Exception {
    int dataBlockSizeBytes = 256;
    final OffHeapGorillaSegment segment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);
    segment.createSegment(SEGMENT_TIMESTAMP);
    // fill up the header till we hit the next block
    for (int i = 0; i < 26; i++) {
      segment.writeData(i, 64);
      segment.updateHeader();
    }

    // have to reset
    segment.reset();
    assertThrows(IllegalArgumentException.class, () -> {
      segment.readData(-1);
    });
    assertThrows(IllegalArgumentException.class, () -> {
      segment.readData(65);
    });

    for (int i = 0; i < 26; i++) {
      assertEquals(i, segment.readData(64));
    }
  }

  @Test
  public void readAfterWrite(@Mocked MetricRegistry registry,
                             @Mocked Gauge memoryBlockCountGauge,
                             @Mocked Gauge segmentLengthGauge) {
    int dataBlockSizeBytes = 256;
    final OffHeapGorillaSegment segment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);
    segment.createSegment(SEGMENT_TIMESTAMP);

    segment.writeData(42L, 64);
    assertThrows(IllegalStateException.class, () -> {
      segment.readData(64);
    });
  }

  @Test
  public void writeAfterRead(@Mocked MetricRegistry registry,
                             @Mocked Gauge memoryBlockCountGauge,
                             @Mocked Gauge segmentLengthGauge) throws Exception {
    int dataBlockSizeBytes = 256;
    final OffHeapGorillaSegment segment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);
    segment.createSegment(SEGMENT_TIMESTAMP);

    segment.writeData(42L, 64);
    segment.reset();
    assertEquals(42, segment.readData(64));
    assertThrows(IllegalStateException.class, () -> {
      segment.writeData(24L, 64);
    });
  }

  @Test
  void freeSegmentWithOneDataBlocks(
      @Mocked MetricRegistry registry,
      @Mocked Gauge memoryBlockCountGauge,
      @Mocked Gauge segmentLengthGauge) {

    new Expectations() {
      {
        registry.gauge("memory.block.count");
        result = memoryBlockCountGauge;

        registry.gauge("segment.length");
        result = segmentLengthGauge;
      }
    };

    int ts = (int) (now / 1000);
    int segmentTime = ts - (ts % SECONDS_IN_A_TIMESERIES % SECONDS_IN_A_SEGMENT);

    int dataBlockSizeBytes = 256;
    OffHeapGorillaSegment segment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);

    segment.collectMetrics();
    new Verifications() {
      {
        long memoryBlockCount;
        long segmentBytes;
        memoryBlockCountGauge.set(memoryBlockCount = withCapture());
        segmentLengthGauge.set(segmentBytes = withCapture());

        assertEquals(0, memoryBlockCount);
        assertEquals(0, segmentBytes);
      }
    };

    segment.createSegment(segmentTime);

    segment.collectMetrics();
    new Verifications() {
      {
        long memoryBlockCount;
        long segmentBytes;
        memoryBlockCountGauge.set(memoryBlockCount = withCapture());
        segmentLengthGauge.set(segmentBytes = withCapture());

        assertEquals(1, memoryBlockCount);
        assertEquals(1 * dataBlockSizeBytes, segmentBytes);
      }
    };

    double value = random.nextDouble();
    segment.writeData(segmentTime, 64);
    segment.writeData(Double.doubleToRawLongBits(value), 64);
    segment.updateHeader();

    segment.collectMetrics();
    new Verifications() {
      {
        long memoryBlockCount;
        long segmentBytes;
        memoryBlockCountGauge.set(memoryBlockCount = withCapture());
        segmentLengthGauge.set(segmentBytes = withCapture());

        assertEquals(1, memoryBlockCount);
        assertEquals(1 * dataBlockSizeBytes, segmentBytes);
      }
    };

    segment.free();

    segment.collectMetrics();
    new Verifications() {
      {
        long memoryBlockCount;
        long segmentBytes;
        memoryBlockCountGauge.set(memoryBlockCount = withCapture());
        segmentLengthGauge.set(segmentBytes = withCapture());

        assertEquals(0, memoryBlockCount);
        assertEquals(0, segmentBytes);
      }
    };
  }

  @Test
  void freeSegmentWithMultipleDataBlocks(
      @Mocked MetricRegistry registry,
      @Mocked Gauge memoryBlockCountGauge,
      @Mocked Gauge segmentLengthGauge) {

    new Expectations() {
      {
        registry.gauge("memory.block.count");
        result = memoryBlockCountGauge;

        registry.gauge("segment.length");
        result = segmentLengthGauge;
      }
    };

    int ts = (int) (now / 1000);
    int size = 100;
    int segmentTime = ts - (ts % SECONDS_IN_A_TIMESERIES % SECONDS_IN_A_SEGMENT);

    int dataBlockSizeBytes = 256;
    OffHeapGorillaSegment segment = new OffHeapGorillaSegment(dataBlockSizeBytes, registry);

    segment.collectMetrics();
    new Verifications() {
      {
        long memoryBlockCount;
        long segmentBytes;
        memoryBlockCountGauge.set(memoryBlockCount = withCapture());
        segmentLengthGauge.set(segmentBytes = withCapture());

        assertEquals(0, memoryBlockCount);
        assertEquals(0, segmentBytes);
      }
    };

    segment.createSegment(segmentTime);

    segment.collectMetrics();
    int initialBlockCount = 1;
    int initialSegmentBytes = initialBlockCount * dataBlockSizeBytes;

    new Verifications() {
      {
        long memoryBlockCount;
        long segmentBytes;
        memoryBlockCountGauge.set(memoryBlockCount = withCapture());
        segmentLengthGauge.set(segmentBytes = withCapture());

        assertEquals(initialBlockCount, memoryBlockCount);
        assertEquals(initialSegmentBytes, segmentBytes);
      }
    };

    for (int i = 0; i < size; i++) {
      int time = segmentTime + i;
      double value = random.nextDouble();
      segment.writeData(time, 64);
      segment.writeData(Double.doubleToRawLongBits(value), 64);
      segment.updateHeader();
    }

    segment.collectMetrics();
    new Verifications() {
      {
        long memoryBlockCount;
        long segmentBytes;
        memoryBlockCountGauge.set(memoryBlockCount = withCapture());
        segmentLengthGauge.set(segmentBytes = withCapture());

        assertTrue(memoryBlockCount > initialBlockCount);
        assertEquals(memoryBlockCount * dataBlockSizeBytes, segmentBytes);
      }
    };

    segment.openSegment(segment.getAddress());
    segment.free();

    segment.collectMetrics();
    new Verifications() {
      {
        long memoryBlockCount;
        long segmentBytes;
        memoryBlockCountGauge.set(memoryBlockCount = withCapture());
        segmentLengthGauge.set(segmentBytes = withCapture());

        assertEquals(0, memoryBlockCount);
        assertEquals(0, segmentBytes);
      }
    };
  }

}
