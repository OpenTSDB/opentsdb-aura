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
import mockit.Injectable;
import mockit.Verifications;
import net.opentsdb.aura.metrics.core.SegmentCollector;
import net.opentsdb.aura.metrics.core.BasicTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderType;
import net.opentsdb.aura.metrics.core.data.MemoryBlock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static mockit.internal.expectations.ActiveInvocations.times;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GorillaTimeSeriesEncoderTest {

  private static final int SEGMENT_TIMESTAMP = 1620237600;
  private static final int SEGMENT_SIZE_HOUR = 1;
  private static final int SECONDS_IN_A_SEGMENT = (int) TimeUnit.HOURS.toSeconds(SEGMENT_SIZE_HOUR);

  private static GorillaTimeSeriesEncoder encoder;
  private static SegmentCollector segmentCollector;
  private static MetricRegistry metricRegistry;
  private Random random = new Random(System.currentTimeMillis());

  @BeforeAll
  static void setUp() {
    metricRegistry = new MetricRegistry();
    segmentCollector =
        new SegmentCollector(
            10, 15, new OffHeapGorillaSegment(256, metricRegistry), metricRegistry);
  }

  @BeforeEach
  void before() {
    encoder =
        new GorillaTimeSeriesEncoder(
            false,
            metricRegistry,
            new OffHeapGorillaSegment(256, metricRegistry),
            segmentCollector);
  }

  @Test
  void ctoreAndCoverage() {
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoint(SEGMENT_TIMESTAMP + 60, 42.5);
    double[] values = new double[SECONDS_IN_A_SEGMENT];
    encoder.readAndDedupe(values);
    assertEquals(42.5, values[60], 0.001);

    encoder.collectSegment(42);
    new Verifications() {
      {
        times(1);
        segmentCollector.collect(42);
      }
    };

    encoder.freeSegment();
    new Verifications() {
      {
        times(1);
        segmentCollector.freeSegments();
      }
    };

    encoder.freeCollectedSegments();
    new Verifications() {
      {
        times(2);
        segmentCollector.freeSegments();
      }
    };

    encoder.setTags(new String[0]);
    new Verifications() {
      {
        times(1);
        segmentCollector.setTags(new String[0]);
      }
    };
  }

  @Test
  void addFirstDataPoint() {
    int ts = SEGMENT_TIMESTAMP;
    ts = SEGMENT_TIMESTAMP + 60;
    double value = 42D;

    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoint(ts, value);

    int[] tArray = new int[1];
    double[] vArray = new double[1];
    final AtomicInteger ai = new AtomicInteger(0);

    encoder.read(
        (t, v, d) -> {
          int i = ai.get();
          tArray[i] = t;
          vArray[i] = v;
          ai.getAndIncrement();
        });

    assertEquals(ts, tArray[0]);
    assertEquals(value, vArray[0]);

    BasicGorillaSegment segment = encoder.getSegment();
    assertEquals(SEGMENT_TIMESTAMP, encoder.getSegmentTime());
    assertEquals(value, Double.longBitsToDouble(segment.getLastValue()), 0.001);
    assertEquals(ts, segment.getLastTimestamp());
    assertEquals(60, segment.getLastTimestampDelta());
    assertEquals(64, segment.getLastValueLeadingZeros());
    assertEquals(64, segment.getLastValueTrailingZeros());
    assertEquals(1, segment.getNumDataPoints());
    assertTrue(encoder.segmentIsDirty());
    assertFalse(encoder.segmentHasOutOfOrderOrDuplicates());

    // reload to validate the dirty flag stays
    long segmentAddress = ((MemoryBlock) segment).getAddress();
    before();
    encoder.openSegment(segmentAddress);

    assertEquals(SEGMENT_TIMESTAMP, encoder.getSegmentTime());
    assertEquals(value, Double.longBitsToDouble(segment.getLastValue()), 0.001);
    assertEquals(ts, segment.getLastTimestamp());
    assertEquals(60, segment.getLastTimestampDelta());
    assertEquals(64, segment.getLastValueLeadingZeros());
    assertEquals(64, segment.getLastValueTrailingZeros());
    assertEquals(1, segment.getNumDataPoints());
    assertTrue(encoder.segmentIsDirty());
    assertFalse(encoder.segmentHasOutOfOrderOrDuplicates());
  }

  @Test
  void addNextDataPoint() {
    int ts = SEGMENT_TIMESTAMP;
    int size = 100;
    int[] times = new int[size];
    double[] values = new double[size];

    encoder.createSegment(SEGMENT_TIMESTAMP);

    for (int i = 0; i < size; i++) {
      int time = ts + i;
      double value = random.nextDouble();
      times[i] = time;
      values[i] = value;
      encoder.addDataPoint(time, value);
    }

    int[] tArray = new int[size];
    double[] vArray = new double[size];
    final AtomicInteger ai = new AtomicInteger(0);
    encoder.read(
        (t, v, d) -> {
          int i = ai.get();
          tArray[i] = t;
          vArray[i] = v;
          ai.getAndIncrement();
        });

    assertArrayEquals(times, tArray);
    assertArrayEquals(values, vArray);

    BasicGorillaSegment segment = encoder.getSegment();
    assertEquals(SEGMENT_TIMESTAMP, encoder.getSegmentTime());
    assertEquals(values[values.length - 1], Double.longBitsToDouble(segment.getLastValue()), 0.001);
    assertEquals(times[times.length - 1], segment.getLastTimestamp());
    assertEquals(size, segment.getNumDataPoints());
    assertTrue(encoder.segmentIsDirty());
    assertFalse(encoder.segmentHasOutOfOrderOrDuplicates());
  }

  @Test
  void testEncoding() {
    int[] times =
        new int[] {
          1611288000,
          1611288001,
          1611288002,
          1611288003,
          1611288004,
          1611288005,
          1611288006,
          1611288007,
          1611288008,
          1611288009,
          1611288010,
          1611288011,
          1611288012,
          1611288013,
          1611288014,
          1611288015,
          1611288016,
          1611288017,
          1611288018,
          1611288019,
          1611288020,
          1611288021,
          1611288022,
          1611288023,
          1611288024
        };
    double[] values =
        new double[] {
          0.8973950653568595,
          0.43654855440361706,
          0.827450779634358,
          0.3584920510780665,
          0.9295624657514724,
          0.9610921547553934,
          0.6329804921575314,
          0.34905996592724153,
          0.5379730703355181,
          0.8403559626106764,
          0.30075147324566376,
          0.15691026481149195,
          0.7525354276869367,
          0.942970394430076,
          0.2401190623680185,
          0.42611404794594654,
          0.7615746658524079,
          0.46418976228229414,
          0.6942765189361159,
          0.9690728790734268,
          0.32890435244089244,
          0.6098703276841767,
          0.22878432168195317,
          0.8844305249065624,
          0.7157591580282211
        };

    int numPoints = values.length;

    encoder.createSegment(1611288000);

    for (int i = 0; i < numPoints; i++) {
      encoder.addDataPoint(times[i], values[i]);
    }

    int[] tArray = new int[numPoints];
    double[] vArray = new double[numPoints];
    final AtomicInteger ai = new AtomicInteger(0);
    encoder.read(
        (t, v, d) -> {
          int i = ai.get();
          tArray[i] = t;
          vArray[i] = v;
          ai.getAndIncrement();
        });

    assertArrayEquals(times, tArray);
    assertArrayEquals(values, vArray);
  }

  @Test
  void testOutOfOrder() throws Exception {
    int[] times =
        new int[] {
          1611288000, 1611288002, 1611288003, 1611288001,
        };
    double[] values =
        new double[] {
          0.8973950653568595, 0.43654855440361706, 0.827450779634358, 0.3584920510780665,
        };

    int numPoints = values.length;

    int segmentTime = 1611288000;
    encoder.createSegment(segmentTime);

    for (int i = 0; i < numPoints; i++) {
      encoder.addDataPoint(times[i], values[i]);
    }

    int[] tArray = new int[numPoints];
    double[] vArray = new double[numPoints];
    final AtomicInteger ai = new AtomicInteger(0);
    encoder.read(
        (t, v, d) -> {
          int i = ai.get();
          tArray[i] = t;
          vArray[i] = v;
          ai.getAndIncrement();
        });

    assertArrayEquals(times, tArray);
    assertArrayEquals(values, vArray);
    BasicGorillaSegment segment = encoder.getSegment();
    assertEquals(segmentTime, encoder.getSegmentTime());
    assertEquals(values[values.length - 1], Double.longBitsToDouble(segment.getLastValue()), 0.001);
    assertEquals(times[times.length - 1], segment.getLastTimestamp());
    assertEquals(numPoints, segment.getNumDataPoints());
    assertTrue(encoder.segmentIsDirty());
    assertTrue(encoder.segmentHasOutOfOrderOrDuplicates());

    // reload to validate the OOO flag stays
    long segmentAddress = ((MemoryBlock) segment).getAddress();
    before();
    encoder.openSegment(segmentAddress);

    assertEquals(segmentTime, encoder.getSegmentTime());
    assertEquals(numPoints, segment.getNumDataPoints());
    assertTrue(encoder.segmentIsDirty());
    assertTrue(encoder.segmentHasOutOfOrderOrDuplicates());

    // reset and read
    //    encoder.reset();
    ai.set(0);
    encoder.read(
        (t, v, d) -> {
          int i = ai.get();
          tArray[i] = t;
          vArray[i] = v;
          ai.getAndIncrement();
        });
    assertEquals(segmentTime, encoder.getSegmentTime());
    assertEquals(numPoints, segment.getNumDataPoints());
    assertTrue(encoder.segmentIsDirty());
    assertTrue(encoder.segmentHasOutOfOrderOrDuplicates());
  }

  @Test
  void testDupes() throws Exception {
    int[] times =
        new int[] {
          1611288000, 1611288002, 1611288002, 1611288004,
        };
    double[] values =
        new double[] {
          0.8973950653568595, 0.43654855440361706, 0.827450779634358, 0.3584920510780665,
        };

    int numPoints = values.length;

    int segmentTime = 1611288000;
    encoder.createSegment(segmentTime);

    for (int i = 0; i < numPoints; i++) {
      encoder.addDataPoint(times[i], values[i]);
    }

    int[] tArray = new int[numPoints];
    double[] vArray = new double[numPoints];
    final AtomicInteger ai = new AtomicInteger(0);
    encoder.read(
        (t, v, d) -> {
          int i = ai.get();
          tArray[i] = t;
          vArray[i] = v;
          ai.getAndIncrement();
        });

    assertArrayEquals(times, tArray);
    assertArrayEquals(values, vArray);
    BasicGorillaSegment segment = encoder.getSegment();
    assertEquals(segmentTime, encoder.getSegmentTime());
    assertEquals(values[values.length - 1], Double.longBitsToDouble(segment.getLastValue()), 0.001);
    assertEquals(times[times.length - 1], segment.getLastTimestamp());
    assertEquals(numPoints, segment.getNumDataPoints());
    assertTrue(encoder.segmentIsDirty());
    assertTrue(encoder.segmentHasOutOfOrderOrDuplicates());
  }

  @Test
  void testMarkFlushed() {
    int ts = SEGMENT_TIMESTAMP;
    ts += 60;
    double value = 42D;

    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoint(ts, value);

    int[] tArray = new int[1];
    double[] vArray = new double[1];
    final AtomicInteger ai = new AtomicInteger(0);

    encoder.read(
        (t, v, d) -> {
          int i = ai.get();
          tArray[i] = t;
          vArray[i] = v;
          ai.getAndIncrement();
        });

    assertEquals(ts, tArray[0]);
    assertEquals(value, vArray[0]);

    BasicGorillaSegment segment = encoder.getSegment();
    assertEquals(SEGMENT_TIMESTAMP, encoder.getSegmentTime());
    assertEquals(value, Double.longBitsToDouble(segment.getLastValue()), 0.001);
    assertEquals(ts, segment.getLastTimestamp());
    assertEquals(60, segment.getLastTimestampDelta());
    assertEquals(64, segment.getLastValueLeadingZeros());
    assertEquals(64, segment.getLastValueTrailingZeros());
    assertEquals(1, segment.getNumDataPoints());
    assertTrue(encoder.segmentIsDirty());
    assertFalse(encoder.segmentHasOutOfOrderOrDuplicates());

    encoder.markSegmentFlushed();
    assertFalse(encoder.segmentIsDirty());

    // reload to validate the dirty flag stays
    long segmentAddress = ((MemoryBlock) segment).getAddress();
    before();
    encoder.openSegment(segmentAddress);

    assertEquals(SEGMENT_TIMESTAMP, encoder.getSegmentTime());
    assertEquals(value, Double.longBitsToDouble(segment.getLastValue()), 0.001);
    assertEquals(ts, segment.getLastTimestamp());
    assertEquals(60, segment.getLastTimestampDelta());
    assertEquals(64, segment.getLastValueLeadingZeros());
    assertEquals(64, segment.getLastValueTrailingZeros());
    assertEquals(1, segment.getNumDataPoints());
    assertFalse(encoder.segmentIsDirty());
    assertFalse(encoder.segmentHasOutOfOrderOrDuplicates());
  }

  @Test
  void freeSegment(@Injectable Gauge segmentCountGauge) {

    MetricRegistry registry = new MetricRegistry();

    new Expectations(registry) {
      {
        registry.gauge("segment.count");
        result = segmentCountGauge;
      }
    };

    int ts = SEGMENT_TIMESTAMP;
    BasicTimeSeriesEncoder encoder =
        new GorillaTimeSeriesEncoder(
            false, registry, new OffHeapGorillaSegment(256, registry), segmentCollector);

    encoder.collectMetrics();
    new Verifications() {
      {
        long segmentCount;
        segmentCountGauge.set(segmentCount = withCapture());
        assertEquals(0, segmentCount);
      }
    };

    long a1 = encoder.createSegment(ts);
    encoder.createSegment(ts);
    encoder.createSegment(ts);

    encoder.collectMetrics();
    new Verifications() {
      {
        long segmentCount;
        segmentCountGauge.set(segmentCount = withCapture());
        assertEquals(3, segmentCount);
      }
    };

    encoder.openSegment(a1);
    encoder.freeSegment();

    encoder.collectMetrics();
    new Verifications() {
      {
        long segmentCount;
        segmentCountGauge.set(segmentCount = withCapture());
        assertEquals(segmentCount, 2);
      }
    };
  }

  @Test
  void testLossyManyRandom() {
    encoder =
        new GorillaTimeSeriesEncoder(
            true, metricRegistry, new OffHeapGorillaSegment(256, metricRegistry), segmentCollector);

    int runs = 4096;
    int maxSize = 3600;

    for (int x = 0; x < runs; x++) {
      int ts = SEGMENT_TIMESTAMP;
      encoder.createSegment(SEGMENT_TIMESTAMP);

      int size = random.nextInt(maxSize);
      if (size == 0) {
        size = 1;
      }
      Map<Integer, Double> canonical = new TreeMap<>();
      int time = SEGMENT_TIMESTAMP;
      for (int i = 0; i < size; i++) {
        time += (random.nextInt(2 * 60));
        if (time > SEGMENT_TIMESTAMP + (3600)) {
          break;
        }
        // NOTE - We have to limit the integer portion a fair bit.
        double value = random.nextInt(4096) + random.nextDouble();
        encoder.addDataPoint(time, value);
        canonical.put(time, value);
      }

      Map<Integer, Double> tested = new TreeMap<>();
      encoder.read((t, v, d) -> tested.put(t, v));
      assertLossyMapEquals(canonical, tested);

      encoder.freeSegment();
    }
  }

  @Test
  public void writeMoreThanShortDps() throws Exception {
    // what happens if we write more than Short.MAX_VALUE dps?
    int ts = SEGMENT_TIMESTAMP;
    encoder.createSegment(SEGMENT_TIMESTAMP);
    for (int i = 0; i < ((int) Short.MAX_VALUE); i++) {
      encoder.addDataPoint(ts++, i);
    }
    assertThrows(
        IllegalStateException.class,
        () -> {
          encoder.addDataPoint(SEGMENT_TIMESTAMP, -1);
        });
  }

  @Test
  void testSerializationLosslessManyRandom() throws Exception {
    int runs = 4096;
    int maxSize = 3600;

    for (int x = 0; x < runs; x++) {
      int ts = SEGMENT_TIMESTAMP;
      encoder =
          new GorillaTimeSeriesEncoder(
              false,
              metricRegistry,
              new OffHeapGorillaSegment(256, metricRegistry),
              segmentCollector);
      encoder.createSegment(SEGMENT_TIMESTAMP);

      int size = random.nextInt(maxSize);
      if (size == 0) {
        size = 1;
      }
      Map<Integer, Double> canonical = new TreeMap<>();
      int time = SEGMENT_TIMESTAMP;
      for (int i = 0; i < size; i++) {
        time += (random.nextInt(2 * 60));
        if (time > SEGMENT_TIMESTAMP + (3600)) {
          break;
        }
        double value = random.nextLong() + random.nextDouble();
        encoder.addDataPoint(time, value);
        canonical.put(time, value);
      }

      int serializationLength = encoder.serializationLength();
      byte[] buffer = new byte[serializationLength];
      encoder.serialize(buffer, 0, serializationLength);
      assertEquals(TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS, buffer[0]);

      OnHeapGorillaSegment onHeapSegment =
          new OnHeapGorillaSegment(SEGMENT_TIMESTAMP, buffer, 0, serializationLength);
      encoder =
          new GorillaTimeSeriesEncoder(false, metricRegistry, onHeapSegment, segmentCollector);
      Map<Integer, Double> dps = new TreeMap<>();
      encoder.read(
          (t, v, d) -> {
            dps.put(t, v);
          });
      assertEquals(canonical, dps);

      encoder.freeSegment();
    }
  }

  @Test
  void testSerializationLossyManyRandom() throws Exception {
    int runs = 4096;
    int maxSize = 3600;

    for (int x = 0; x < runs; x++) {
      int ts = SEGMENT_TIMESTAMP;

      encoder =
          new GorillaTimeSeriesEncoder(
              true,
              metricRegistry,
              new OffHeapGorillaSegment(256, metricRegistry),
              segmentCollector);
      encoder.createSegment(SEGMENT_TIMESTAMP);

      int size = random.nextInt(maxSize);
      if (size == 0) {
        size = 1;
      }
      Map<Integer, Double> canonical = new TreeMap<>();
      int time = SEGMENT_TIMESTAMP;
      for (int i = 0; i < size; i++) {
        time += (random.nextInt(2 * 60));
        if (time > SEGMENT_TIMESTAMP + (3600)) {
          break;
        }
        double value = random.nextInt(4096) + random.nextDouble();
        encoder.addDataPoint(time, value);
        canonical.put(time, value);
      }

      int serializationLength = encoder.serializationLength();
      byte[] buffer = new byte[serializationLength];
      encoder.serialize(buffer, 0, serializationLength);
      assertEquals(TimeSeriesEncoderType.GORILLA_LOSSY_SECONDS, buffer[0]);

      OnHeapGorillaSegment onHeapSegment =
          new OnHeapGorillaSegment(SEGMENT_TIMESTAMP, buffer, 0, serializationLength);
      encoder = new GorillaTimeSeriesEncoder(true, metricRegistry, onHeapSegment, segmentCollector);
      Map<Integer, Double> dps = new TreeMap<>();
      encoder.read(
          (t, v, d) -> {
            dps.put(t, v);
          });
      assertLossyMapEquals(canonical, dps);

      encoder.freeSegment();
    }
  }

  @Test
  void testSerializationSingleValue() throws Exception {
    int ts = SEGMENT_TIMESTAMP;
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoint(SEGMENT_TIMESTAMP, 42D);

    int serializationLength = encoder.serializationLength();
    byte[] buffer = new byte[serializationLength];
    encoder.serialize(buffer, 0, serializationLength);
    assertEquals(TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS, buffer[0]);

    OnHeapGorillaSegment onHeapSegment =
        new OnHeapGorillaSegment(SEGMENT_TIMESTAMP, buffer, 0, serializationLength);
    encoder.setSegment(onHeapSegment);
    Map<Integer, Double> dps = new TreeMap<>();
    encoder.read(
        (t, v, d) -> {
          dps.put(t, v);
        });

    assertEquals(1, dps.size());
    assertEquals(42D, dps.get(SEGMENT_TIMESTAMP));
    encoder.freeSegment();
  }

  @Test
  void testSerializationZeros() throws Exception {
    int ts = SEGMENT_TIMESTAMP;
    encoder.createSegment(SEGMENT_TIMESTAMP);

    Map<Integer, Double> canonical = new TreeMap<>();
    int time = SEGMENT_TIMESTAMP;
    for (int i = 0; i < 32; i++) {
      time += 60;
      encoder.addDataPoint(time, 0);
      canonical.put(time, 0D);
    }

    int serializationLength = encoder.serializationLength();
    byte[] buffer = new byte[serializationLength];
    encoder.serialize(buffer, 0, serializationLength);
    assertEquals(TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS, buffer[0]);

    OnHeapGorillaSegment onHeapSegment =
        new OnHeapGorillaSegment(SEGMENT_TIMESTAMP, buffer, 0, serializationLength);
    encoder = new GorillaTimeSeriesEncoder(false, metricRegistry, onHeapSegment, segmentCollector);
    Map<Integer, Double> dps = new TreeMap<>();
    encoder.read(
        (t, v, d) -> {
          dps.put(t, v);
        });

    assertEquals(canonical, dps);
    encoder.freeSegment();
  }

  @Test
  void testSerializationEmpty() throws Exception {
    int ts = SEGMENT_TIMESTAMP;
    encoder.createSegment(SEGMENT_TIMESTAMP);

    int serializationLength = encoder.serializationLength();
    byte[] buffer = new byte[serializationLength];
    encoder.serialize(buffer, 0, serializationLength);
    assertEquals(TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS, buffer[0]);
    assertArrayEquals(new byte[] {0, 0}, buffer);

    OnHeapGorillaSegment onHeapSegment =
        new OnHeapGorillaSegment(SEGMENT_TIMESTAMP, buffer, 0, serializationLength);
    encoder = new GorillaTimeSeriesEncoder(false, metricRegistry, onHeapSegment, segmentCollector);

    Map<Integer, Double> dps = new TreeMap<>();
    encoder.read(
        (t, v, d) -> {
          dps.put(t, v);
        });
    assertTrue(dps.isEmpty());

    encoder.freeSegment();
  }

  @Test
  void testSerializationBufferOffset() throws Exception {
    int ts = SEGMENT_TIMESTAMP;
    encoder.createSegment(SEGMENT_TIMESTAMP);

    Map<Integer, Double> canonical = new TreeMap<>();
    int time = SEGMENT_TIMESTAMP;
    for (int i = 0; i < 64; i++) {
      time += (random.nextInt(2 * 60));
      if (time > SEGMENT_TIMESTAMP + (3600)) {
        break;
      }
      double value = random.nextLong() + random.nextDouble();
      encoder.addDataPoint(time, value);
      canonical.put(time, value);
    }

    int serializationLength = encoder.serializationLength();
    byte[] buffer = new byte[serializationLength * 2];
    Arrays.fill(buffer, 0, buffer.length, (byte) -1);

    int offset = 13;
    encoder.serialize(buffer, offset, serializationLength);

    OnHeapGorillaSegment onHeapSegment =
        new OnHeapGorillaSegment(SEGMENT_TIMESTAMP, buffer, offset, serializationLength);
    encoder = new GorillaTimeSeriesEncoder(false, metricRegistry, onHeapSegment, segmentCollector);
    Map<Integer, Double> dps = new TreeMap<>();
    encoder.read(
        (t, v, d) -> {
          dps.put(t, v);
        });
    assertEquals(canonical, dps);

    encoder.freeSegment();
  }

  @Test
  void testSerializationBufferTooSmall() throws Exception {
    int ts = SEGMENT_TIMESTAMP;
    encoder.createSegment(SEGMENT_TIMESTAMP);

    Map<Integer, Double> canonical = new TreeMap<>();
    int time = SEGMENT_TIMESTAMP;
    for (int i = 0; i < 64; i++) {
      time += (random.nextInt(2 * 60));
      if (time > SEGMENT_TIMESTAMP + (3600)) {
        break;
      }
      double value = random.nextLong() + random.nextDouble();
      encoder.addDataPoint(time, value);
      canonical.put(time, value);
    }

    int serializationLength = encoder.serializationLength();
    byte[] buffer = new byte[serializationLength - 1];
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> {
          encoder.serialize(buffer, 0, serializationLength);
        });

    encoder.freeSegment();
  }

  @Test
  void testSerializationFullSegment() throws Exception {
    int ts = SEGMENT_TIMESTAMP;
    encoder =
        new GorillaTimeSeriesEncoder(
            false,
            metricRegistry,
            new OffHeapGorillaSegment(256, metricRegistry),
            segmentCollector);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    for (int i = 0; i < SECONDS_IN_A_SEGMENT; i++) {
      encoder.addDataPoint(ts++, i);
    }

    assertEquals(SECONDS_IN_A_SEGMENT, encoder.getNumDataPoints());
    int expectedLength = 8563;
    assertEquals(expectedLength, encoder.serializationLength());

    byte[] buffer = new byte[expectedLength];
    encoder.serialize(buffer, 0, expectedLength);
    encoder.freeSegment();

    OnHeapGorillaSegment onHeapSegment =
        new OnHeapGorillaSegment(SEGMENT_TIMESTAMP, buffer, 0, 8567);
    encoder = new GorillaTimeSeriesEncoder(false, metricRegistry, onHeapSegment, segmentCollector);
    double[] array = new double[SECONDS_IN_A_SEGMENT];
    encoder.readAndDedupe(array);
    for (int i = 0; i < array.length; i++) {
      assertEquals(i, array[i], 0.001);
    }
  }

  @Test
  void testReadAndDedupeExceedsSegmentWidth() throws Exception {
    // This test will currently throw an OOB exception because we've
    // written data beyond the segment end time.
    int ts = SEGMENT_TIMESTAMP + SECONDS_IN_A_SEGMENT - 5;
    encoder.createSegment(SEGMENT_TIMESTAMP);
    for (int i = 0; i < 10; i++) {
      encoder.addDataPoint(ts++, i);
    }

    double[] read = new double[SECONDS_IN_A_SEGMENT];
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> {
          encoder.readAndDedupe(read);
        });
  }

  @Test
  public void testReadAndDedupeEmptySegment() throws Exception {
    encoder.createSegment(SEGMENT_TIMESTAMP);
    double[] values = new double[SECONDS_IN_A_SEGMENT];
    Arrays.fill(values, Double.NaN);
    assertEquals(0, encoder.readAndDedupe(values));
    for (int i = 0; i < values.length; i++) {
      assertTrue(Double.isNaN(values[i]));
    }
  }

  //  @Test
  void perfTest() {
    //    int[] times = new int[]{1610413200, 1610413201, 1610413202, 1610413203, 1610413204,
    // 1610413205, 1610413206, 1610413207, 1610413208, 1610413209, 1610413210, 1610413211,
    // 1610413212, 1610413213, 1610413214, 1610413215, 1610413216, 1610413217, 1610413218,
    // 1610413219, 1610413220, 1610413221, 1610413222, 1610413223, 1610413224, 1610413225,
    // 1610413226, 1610413227, 1610413228, 1610413229, 1610413230, 1610413231, 1610413232,
    // 1610413233, 1610413234, 1610413235, 1610413236, 1610413237, 1610413238, 1610413239,
    // 1610413240, 1610413241, 1610413242, 1610413243, 1610413244, 1610413245, 1610413246,
    // 1610413247, 1610413248, 1610413249, 1610413250, 1610413251, 1610413252, 1610413253,
    // 1610413254, 1610413255, 1610413256, 1610413257, 1610413258, 1610413259, 1610413260,
    // 1610413261, 1610413262, 1610413263, 1610413264, 1610413265, 1610413266, 1610413267,
    // 1610413268, 1610413269, 1610413270, 1610413271, 1610413272, 1610413273, 1610413274,
    // 1610413275, 1610413276, 1610413277, 1610413278, 1610413279, 1610413280, 1610413281,
    // 1610413282, 1610413283, 1610413284, 1610413285, 1610413286, 1610413287, 1610413288,
    // 1610413289, 1610413290, 1610413291, 1610413292, 1610413293, 1610413294, 1610413295,
    // 1610413296, 1610413297, 1610413298, 1610413299};
    //    double[] values = new double[]{0.6544543964944665, 0.13654265289119372,
    // 0.7721472887508797, 0.44676921503780764, 0.49873266340193156, 0.3563209369238085,
    // 0.1712787017573496, 0.17245604452699526, 0.34735698530181325, 0.4790019511365192,
    // 0.267547369244868, 0.2804964781435323, 0.6700412981802677, 0.0818929376289923,
    // 0.2256159015024526, 0.35040946531693773, 0.2316777130722183, 0.9918684980591985,
    // 0.26596862128076726, 0.41995105643983066, 0.8017380159519977, 0.5275985223865598,
    // 0.3901941130615747, 0.5503714851833473, 0.7519510333706096, 0.9975832423575988,
    // 0.8317170705632195, 0.7147552505277279, 0.6699724623939557, 0.8173069448221795,
    // 0.457840593859697, 0.927931474078969, 0.7583641402790772, 0.47698609341544174,
    // 0.006185713269531656, 0.04523167477940948, 0.7110526640856188, 0.9908019533010175,
    // 0.4770918118763601, 0.19740996405392663, 0.9635374433427876, 0.3891716219928121,
    // 0.22326853047825346, 0.40033096031009885, 0.6984709251867834, 0.27752532520896067,
    // 0.27896581856123703, 0.4805688033679474, 0.7548348030330299, 0.478222282989228,
    // 0.25315550286795596, 0.09185064445435698, 0.6367874291013111, 0.29665113622857553,
    // 0.6746944256339229, 0.26231576823734404, 0.1473597406968461, 0.008225143745897556,
    // 0.022165604271503048, 0.6088522257977567, 0.7825280813340109, 0.7498966530820201,
    // 0.7970707282538214, 0.08231020334841521, 0.912794004600772, 0.1179786862248563,
    // 0.9062732811814403, 0.8661819072263801, 0.9522528950544387, 0.42347657640344527,
    // 0.30806887677697004, 0.8413756252442925, 0.16923463898291724, 0.7367957335873322,
    // 0.3459853148354045, 0.8510016707689627, 0.5279508967811399, 0.5656062763439422,
    // 0.44539191525716626, 0.8516259118709604, 0.9671858408633606, 0.25280891304684394,
    // 0.8802238486486609, 0.9212369890050944, 0.8199178611506915, 0.9559608891326044,
    // 0.1306041493239818, 0.009210900630765684, 0.656861893088123, 0.37518466826647534,
    // 0.9468721572477078, 0.2598471090091471, 0.6972734854844218, 0.23349598425198603,
    // 0.6115042713769594, 0.718588070383211, 0.07195725719101687, 0.881652330033378,
    // 0.04763905163076321, 0.2388244445713349};
    //    int[] times = new int[]{1610413260, 1610413261, 1610413262, 1610413263, 1610413264,
    // 1610413265, 1610413266, 1610413267, 1610413268, 1610413269};
    //    double[] values = new double[]{0.7825280813340109, 0.7498966530820201, 0.7970707282538214,
    // 0.08231020334841521, 0.912794004600772, 0.1179786862248563, 0.9062732811814403,
    // 0.8661819072263801, 0.9522528950544387, 0.42347657640344527};

    int[] times =
        new int[] {
          1611288000,
          1611288001,
          1611288002,
          1611288003,
          1611288004,
          1611288005,
          1611288006,
          1611288007,
          1611288008,
          1611288009,
          1611288010,
          1611288011,
          1611288012,
          1611288013,
          1611288014,
          1611288015,
          1611288016,
          1611288017,
          1611288018,
          1611288019,
          1611288020,
          1611288021,
          1611288022,
          1611288023,
          1611288024
        };
    double[] values =
        new double[] {
          0.8973950653568595,
          0.43654855440361706,
          0.827450779634358,
          0.3584920510780665,
          0.9295624657514724,
          0.9610921547553934,
          0.6329804921575314,
          0.34905996592724153,
          0.5379730703355181,
          0.8403559626106764,
          0.30075147324566376,
          0.15691026481149195,
          0.7525354276869367,
          0.942970394430076,
          0.2401190623680185,
          0.42611404794594654,
          0.7615746658524079,
          0.46418976228229414,
          0.6942765189361159,
          0.9690728790734268,
          0.32890435244089244,
          0.6098703276841767,
          0.22878432168195317,
          0.8844305249065624,
          0.7157591580282211
        };

    //    int ts = (int) (now / 1000);
    int numPoints = values.length;
    encoder.createSegment(1611288000);

    int iteration = 1;

    long start = System.nanoTime();

    for (int i = 0; i < iteration; i++) {
      //      encoder.reset();
      for (int j = 0; j < numPoints; j++) {
        encoder.addDataPoint(times[j], values[j]);
      }
    }

    int[] tArray = new int[numPoints];
    double[] vArray = new double[numPoints];
    final AtomicInteger ai = new AtomicInteger(0);
    encoder.read(
        (t, v, d) -> {
          int i = ai.get();
          tArray[i] = t;
          vArray[i] = v;
          ai.getAndIncrement();
        });

    assertArrayEquals(times, tArray);
    assertArrayEquals(values, vArray);

    long end = System.nanoTime();
    System.out.println(
        "Time took for " + iteration + " iterations, " + (end - start) / 1000000 + " ms");
  }

  /**
   * Helper to check the equality of doubles that we expect to be a bit lossy
   *
   * @param expected The expected map values
   * @param test The test map values.
   */
  void assertLossyMapEquals(Map<Integer, Double> expected, Map<Integer, Double> test) {
    if (expected.size() != test.size()) {
      throw new AssertionError(
          "Expected " + expected.size() + " entries but got " + test.size() + " entries.");
    }

    for (Map.Entry<Integer, Double> entry : expected.entrySet()) {
      Double other = test.get(entry.getKey());
      if (other == null) {
        throw new AssertionError(
            "Test was missing an entry for key "
                + entry.getKey()
                + "\nExP: "
                + expected
                + "\ntest: "
                + test);
      }

      assertEquals(entry.getValue(), other, 0.001);
    }
  }
}
