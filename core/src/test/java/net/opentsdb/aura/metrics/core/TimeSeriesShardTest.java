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

import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.data.InSufficientBufferLengthException;
import net.opentsdb.aura.metrics.core.data.Memory;
import net.opentsdb.aura.metrics.core.gorilla.GorillaRawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.OffHeapGorillaRawSegment;
import net.opentsdb.aura.metrics.meta.MetaDataStore;
import io.ultrabrew.metrics.MetricRegistry;
import mockit.Injectable;
import mockit.Verifications;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.hashing.HashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static net.opentsdb.aura.metrics.TestUtil.buildEvent;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimeSeriesShardTest {

  private static ShardConfig config = new ShardConfig();
  private static long now = System.currentTimeMillis();
  private static Random random = new Random(now);
  private static HashFunction hashFunction = new XxHash();

  protected MetricRegistry metricRegistry = new MetricRegistry();
  protected RawTimeSeriesEncoder encoder;
  protected TimeSeriesShardIF shard;
  private TimeseriesStorageContext storageContext;

  @Injectable protected MetaDataStore metaDataStore;
  @Injectable protected MemoryInfoReader memoryInfoReader;
  @Injectable protected ScheduledExecutorService purgeService;
  @Injectable protected SegmentCollector segmentCollector;
  @Injectable protected Flusher flusher;

  private LocalDateTime purgeDateTime = Util.getPurgeTime();

  @BeforeAll
  public static void beforeAll() {
    config.tagTableSize = 5;
    config.metricTableSize = 5;
    config.timeSeriesTableSize = 10;
  }

  @BeforeEach
  public void beforeEach() {
    config.segmentSizeHour = 2;
    encoder =
        new GorillaRawTimeSeriesEncoder(
            false,
            metricRegistry,
            new OffHeapGorillaRawSegment(256, metricRegistry),
            segmentCollector);
    storageContext = new LongRunningStorage.LongRunningStorageContext(config);
    shard =
        new TimeSeriesShard(
            0,
            config,
            storageContext,
            encoder,
            metaDataStore,
            memoryInfoReader,
            metricRegistry,
            purgeDateTime,
            hashFunction,
            flusher,
            purgeService);
  }

  @Test
  void addMultipleDataPoint() {

    String metric = "request.count";
    Map<String, String> tags =
        new HashMap() {
          {
            put("host", "host1");
          }
        };

    int ts = (int) (now / 1000);
    int count = 100;
    int[] times = new int[count];
    double[] values = new double[count];

    for (int i = 0; i < count; i++) {
      times[i] = ts + i;
      values[i] = random.nextDouble();
    }

    LowLevelMetricData.HashedLowLevelMetricData event = buildEvent(metric, tags, times, values);

    shard.addEvent(event);

    int[] decodedTimes = new int[count];
    double[] decodedValues = new double[count];
    AtomicInteger index = new AtomicInteger();

    long timeSeriesHash = event.timeSeriesHash();
    shard.read(
        ts,
        ts + count,
        timeSeriesHash,
        (t, v, d) -> {
          decodedTimes[index.get()] = t;
          decodedValues[index.get()] = v;
          index.getAndIncrement();
        });

    assertArrayEquals(times, decodedTimes);
    assertArrayEquals(values, decodedValues);

    assertEquals(tags.size(), shard.getTagCount(timeSeriesHash));
    byte[] pointerBuffer = new byte[12];
    boolean tagPointerFound = shard.getTagPointer(event.tagsSetHash(), pointerBuffer);
    assertTrue(tagPointerFound);

    long tagAddress = ByteArrays.getLong(pointerBuffer, 0);
    int tagLength = ByteArrays.getInt(pointerBuffer, 8);
    byte[] decodedTag = new byte[tagLength];
    Memory.read(tagAddress, decodedTag, tagLength);

    assertArrayEquals(event.tagsBuffer(), decodedTag);
  }

  @Test
  void addMultipleTimeSeries() {

    String metric = "request.count";
    Map<String, String> tags1 =
        new HashMap() {
          {
            put("host", "host1");
          }
        };
    Map<String, String> tags2 =
        new HashMap() {
          {
            put("host", "host2");
          }
        };

    int ts = (int) (now / 1000);
    int count = 100;
    int[] times = new int[count];
    double[] values = new double[count];

    for (int i = 0; i < count; i++) {
      times[i] = ts + i;
      values[i] = random.nextDouble();
    }

    LowLevelMetricData.HashedLowLevelMetricData event1 = buildEvent(metric, tags1, times, values);
    LowLevelMetricData.HashedLowLevelMetricData event2 = buildEvent(metric, tags2, times, values);

    shard.addEvent(event1);
    shard.addEvent(event2);

    int[] decodedTimes = new int[count];
    double[] decodedValues = new double[count];
    AtomicInteger index = new AtomicInteger();

    long timeSeriesHash1 = event1.timeSeriesHash();
    shard.read(
        ts,
        ts + count,
        timeSeriesHash1,
        (t, v, d) -> {
          decodedTimes[index.get()] = t;
          decodedValues[index.get()] = v;
          index.getAndIncrement();
        });
    assertArrayEquals(times, decodedTimes);
    assertArrayEquals(values, decodedValues);

    index.set(0);
    long timeSeriesHash2 = event2.timeSeriesHash();
    shard.read(
        ts,
        ts + count,
        timeSeriesHash2,
        (t, v, d) -> {
          decodedTimes[index.get()] = t;
          decodedValues[index.get()] = v;
          index.getAndIncrement();
        });

    assertArrayEquals(times, decodedTimes);
    assertArrayEquals(values, decodedValues);

    assertEquals(tags1.size(), shard.getTagCount(timeSeriesHash1));
    byte[] pointerBuffer = new byte[12];
    boolean tagPointer1Found = shard.getTagPointer(event1.tagsSetHash(), pointerBuffer);
    assertTrue(tagPointer1Found);

    long tagAddress = ByteArrays.getLong(pointerBuffer, 0);
    int tagLength = ByteArrays.getInt(pointerBuffer, 8);
    byte[] decodedTag = new byte[tagLength];
    Memory.read(tagAddress, decodedTag, tagLength);

    assertArrayEquals(event1.tagsBuffer(), decodedTag);

    boolean tagPointer2Found = shard.getTagPointer(event2.tagsSetHash(), pointerBuffer);
    assertTrue(tagPointer2Found);

    tagAddress = ByteArrays.getLong(pointerBuffer, 0);
    tagLength = ByteArrays.getInt(pointerBuffer, 8);
    decodedTag = new byte[tagLength];
    Memory.read(tagAddress, decodedTag, tagLength);

    assertArrayEquals(event2.tagsBuffer(), decodedTag);
  }

  @Test
  void purgeStaleSegment() {

    int now = (int) (System.currentTimeMillis() / 1000);
    int currentSegmentTime = storageContext.getSegmentTime(now);
    double value = 54.59;
    int staleTimeStamp =
        (int) (currentSegmentTime - TimeUnit.HOURS.toSeconds(config.retentionHour) - 1);
    int[] timeStamps = new int[config.retentionHour / config.segmentSizeHour + 1];
    double[] values = new double[timeStamps.length];
    timeStamps[0] = staleTimeStamp;
    values[0] = value;
    for (int i = 1; i < timeStamps.length; i++) {
      timeStamps[i] = (int) (staleTimeStamp + TimeUnit.HOURS.toSeconds(i));
      values[i] = value;
    }

    String metric = "request.count";
    Map<String, String> tags =
        new HashMap() {
          {
            put("host", "host1");
          }
        };

    LowLevelMetricData.HashedLowLevelMetricData event = buildEvent(metric, tags, timeStamps, values);
    shard.addEvent(event);

    AtomicInteger index = new AtomicInteger();
    shard.readAll(
        event.timeSeriesHash(),
        (t, v, d) -> {
          index.getAndIncrement();
        });

    assertEquals(timeStamps.length, index.get());

    ((TimeSeriesShard) shard).new PurgeJob().run();

    index.set(0);
    shard.readAll(event.timeSeriesHash(), (t, v, d) -> index.getAndIncrement());
    assertEquals(timeStamps.length - 1, index.get());
  }

  @Test
  void purgeStaleTimeSeries() {
    int now = (int) (System.currentTimeMillis() / 1000);
    int currentSegmentTime = storageContext.getSegmentTime(now);
    int staleTimeStamp =
        (int) (currentSegmentTime - TimeUnit.HOURS.toSeconds(config.retentionHour) - 1);

    int[] timeStamps = new int[] {staleTimeStamp};
    double[] values = new double[] {54.59};

    String metric1 = "request.count";
    String metric2 = "query.count";
    Map<String, String> tags =
        new HashMap() {
          {
            put("host", "host1");
          }
        };

    LowLevelMetricData.HashedLowLevelMetricData eventStale = buildEvent(metric1, tags, timeStamps, values);
    LowLevelMetricData.HashedLowLevelMetricData event = buildEvent(metric2, tags, new int[] {now}, values);
    shard.addEvent(eventStale);
    shard.addEvent(event);

    AtomicInteger index = new AtomicInteger();
    shard.readAll(
        eventStale.timeSeriesHash(),
        (t, v, d) -> {
          index.getAndIncrement();
        });

    assertEquals(timeStamps.length, index.get());

    ((TimeSeriesShard) shard).new PurgeJob().run();

    index.set(0);
    shard.readAll(eventStale.timeSeriesHash(), (t, v, d) -> index.getAndIncrement());
    assertEquals(0, index.get());

    index.set(0);
    shard.readAll(
        event.timeSeriesHash(),
        (t, v, d) -> {
          index.getAndIncrement();
        });
    assertEquals(1, index.get());

    new Verifications() {
      {
        metaDataStore.remove(
            eventStale.tagsSetHash(), withInstanceOf(byte[].class), anyInt, anyInt);
        times = 0;
      }
    };
  }

  @Test
  void purgeStaleTimeSeriesPurgesMeta() throws InSufficientBufferLengthException {
    int now = (int) (System.currentTimeMillis() / 1000);
    int currentSegmentTime = storageContext.getSegmentTime(now);
    int staleTimeStamp =
        (int)
            (currentSegmentTime
                - TimeUnit.HOURS.toSeconds(config.retentionHour + config.segmentSizeHour)
                - 1);

    int[] timeStamps = new int[] {staleTimeStamp};
    double[] values = new double[] {54.59};

    String metric = "request.count";
    Map<String, String> tags =
        new HashMap() {
          {
            put("host", "host1");
            put("region", "east");
          }
        };

    LowLevelMetricData.HashedLowLevelMetricData eventStale = buildEvent(metric, tags, timeStamps, values);
    shard.addEvent(eventStale);

    AtomicInteger index = new AtomicInteger();
    shard.readAll(eventStale.timeSeriesHash(), (t, v, d) -> index.getAndIncrement());

    assertEquals(timeStamps.length, index.get());

    ((TimeSeriesShard) shard).new PurgeJob().run();

    index.set(0);
    shard.readAll(eventStale.timeSeriesHash(), (t, v, d) -> index.getAndIncrement());
    assertEquals(0, index.get());

    new Verifications() {
      {
        byte[] buffer;
        int batchSize;
        long[] docIds;
        metaDataStore.remove(
            docIds = withCapture(), batchSize = withCapture(), buffer = withCapture());
        times = 1;
        byte[] tagset = Util.serializeTags(tags);
        long tagHash = hashFunction.hash(tagset);
        assertEquals(1, batchSize);
        assertEquals(tagHash, docIds[0]);
        assertEquals(tagset.length, buffer.length);
      }
    };
  }

  @Test
  void purgeSegmentsForSparseData() {
    config.segmentSizeHour = 1;
    LongRunningStorage.LongRunningStorageContext storageContext =
        new LongRunningStorage.LongRunningStorageContext(config);
    shard =
        new TimeSeriesShard(
            0,
            config,
            storageContext,
            encoder,
            metaDataStore,
            memoryInfoReader,
            metricRegistry,
            purgeDateTime,
            hashFunction,
            flusher,
            purgeService);

    int now = (int) (System.currentTimeMillis() / 1000);
    int currentSegmentTime = storageContext.getSegmentTime(now);
    int currentTS = (int) (currentSegmentTime + TimeUnit.MINUTES.toSeconds(2));
    double value = 54.59;

    int segmentsInATimeSeries = config.retentionHour / config.segmentSizeHour + 1;
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(config.segmentSizeHour);
    int secondsInATimeSeries = secondsInASegment * segmentsInATimeSeries;

    int t1 = currentTS - secondsInATimeSeries - secondsInASegment * 10;
    int t2 = t1 + secondsInASegment * 4;
    int t3 = t2 + secondsInASegment * 3;
    int t4 = t3 + secondsInASegment * 21;
    int t5 = t4 + secondsInASegment * 5;

    int[] timeStamps = new int[] {t1, t2, t3, t4, t5};
    double[] values = new double[] {value, value, value, value, value};

    String metric = "request.count";
    Map<String, String> tags =
        new HashMap() {
          {
            put("host", "host1");
          }
        };

    LowLevelMetricData.HashedLowLevelMetricData event = buildEvent(metric, tags, timeStamps, values);
    shard.addEvent(event);

    ((TimeSeriesShard) shard).new PurgeJob().run();

    List decodedTimes = new ArrayList(timeStamps.length);
    shard.readAll(
        event.timeSeriesHash(),
        (t, v, d) -> {
          decodedTimes.add(t);
        });

    assertEquals(2, decodedTimes.size());
    assertTrue(decodedTimes.contains(t4));
    assertTrue(decodedTimes.contains(t5));
  }

  @Test
  void purgeStaleSegmentsWithPurgeFrequencyLessThanSegmentTimeWindow() {

    config.segmentSizeHour = 1;
    LongRunningStorage.LongRunningStorageContext storageContext =
        new LongRunningStorage.LongRunningStorageContext(config);
    shard =
        new TimeSeriesShard(
            0,
            config,
            storageContext,
            encoder,
            metaDataStore,
            memoryInfoReader,
            metricRegistry,
            purgeDateTime,
            hashFunction,
            flusher,
            purgeService);

    int now = (int) (System.currentTimeMillis() / 1000);
    int currentSegmentTime = storageContext.getSegmentTime(now);
    int currentTS = (int) (currentSegmentTime + TimeUnit.MINUTES.toSeconds(2));

    int segmentsInATimeSeries = config.retentionHour / config.segmentSizeHour + 1;

    int[] timeStamps = new int[segmentsInATimeSeries];
    double[] values = new double[segmentsInATimeSeries];
    int staleCount = 4;

    for (int i = 0; i < staleCount; i++) {
      timeStamps[i] = (int) (currentTS - TimeUnit.HOURS.toSeconds(i + segmentsInATimeSeries));
      values[i] = 10.2;
    }

    for (int i = staleCount; i < timeStamps.length; i++) {
      timeStamps[i] = (int) (currentTS - TimeUnit.HOURS.toSeconds(i));
      values[i] = 10.2;
    }

    String metric = "request.count";
    Map<String, String> tags =
        new HashMap() {
          {
            put("host", "host1");
          }
        };

    LowLevelMetricData.HashedLowLevelMetricData event = buildEvent(metric, tags, timeStamps, values);
    shard.addEvent(event);

    ((TimeSeriesShard) shard).new PurgeJob().run();

    List decodedTimes = new ArrayList(timeStamps.length);
    shard.readAll(event.timeSeriesHash(), (t, v, d) -> decodedTimes.add(t));

    assertEquals(timeStamps.length - staleCount, decodedTimes.size());
    for (int i = 4; i < timeStamps.length; i++) {
      assertTrue(decodedTimes.contains(timeStamps[i]));
    }
  }

}
