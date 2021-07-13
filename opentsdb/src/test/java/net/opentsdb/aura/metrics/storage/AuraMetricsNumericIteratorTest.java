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

import net.opentsdb.aura.metrics.core.Flusher;
import net.opentsdb.aura.metrics.core.LongRunningStorage;
import net.opentsdb.aura.metrics.core.MemoryInfoReader;
import net.opentsdb.aura.metrics.core.OffHeapTimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.ShardConfig;
import net.opentsdb.aura.metrics.core.StorageMode;
import net.opentsdb.aura.metrics.core.BasicTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.core.TimeSeriesShard;
import net.opentsdb.aura.metrics.core.TimeseriesStorageContext;
import net.opentsdb.aura.metrics.core.XxHash;
import net.opentsdb.aura.metrics.core.gorilla.GorillaSegmentFactory;
import net.opentsdb.aura.metrics.core.gorilla.GorillaTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.GorillaTimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.gorilla.OffHeapGorillaSegmentFactory;
import net.opentsdb.aura.metrics.meta.NewDocStore;
import net.opentsdb.aura.metrics.meta.SharedMetaResult;
import io.ultrabrew.metrics.MetricRegistry;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesQuery;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AuraMetricsNumericIteratorTest {
  private static int BASE_TS;
  private static MockTSDB TSDB;
  private static long tsPointer;
  private static TimeSeriesShardIF shard;
  private static BasicTimeSeriesEncoder encoder;
  private static TimeSeriesRecordFactory timeSeriesRecordFactory;
  private static TimeseriesStorageContext storageContext;
  private static AuraMetricsSourceFactory SOURCE_FACTORY;
  private static TimeSeriesStorageIf TIME_SERIES_STORAGE;
  private static TimeSeriesRecord timeSeriesRecord;
  private static List<Long> SEGMENT_ADDRESSES = new ArrayList<>();
  private static List<Integer> segmentTimes = new ArrayList<>();

  private AuraMetricsQueryResult result;
  private AuraMetricsQueryNode node;

  @BeforeClass
  public static void beforeClass() {
    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    TSDB.registry.initialize(true);

    ShardConfig shardConfig = new ShardConfig();
    shardConfig.segmentSizeHour = 1;
    shardConfig.metaStoreCapacity = 10;

    MetricRegistry registry = new MetricRegistry();
    GorillaSegmentFactory segmentFactory =
        new OffHeapGorillaSegmentFactory(shardConfig.segmentBlockSizeBytes, registry);
    TimeSeriesEncoderFactory<GorillaTimeSeriesEncoder> encoderFactory =
        new GorillaTimeSeriesEncoderFactory(
            false,
            shardConfig.garbageQSize,
            shardConfig.segmentCollectionDelayMinutes,
            registry,
            segmentFactory);

    int segmentsInATimeSeries =
        StorageMode.LONG_RUNNING.getSegmentsPerTimeseries(
            shardConfig.retentionHour, shardConfig.segmentSizeHour);
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(1);

    timeSeriesRecordFactory =
        new OffHeapTimeSeriesRecordFactory(segmentsInATimeSeries, secondsInASegment);

    AuraMetricsNumericArrayIterator.timeSeriesEncoderFactory = encoderFactory;
    AuraMetricsNumericArrayIterator.timeSeriesRecordFactory = timeSeriesRecordFactory;

    encoder = encoderFactory.create();
    MemoryInfoReader memoryInfoReader = mock(MemoryInfoReader.class);
    HashFunction hashFunction = new XxHash();
    NewDocStore docStore =
        new NewDocStore(
            shardConfig.metaStoreCapacity,
            shardConfig.metaPurgeBatchSize,
            null,
            hashFunction,
            mock(SharedMetaResult.class),
            false);
    Flusher flusher = mock(Flusher.class);
    when(flusher.frequency()).thenReturn(30_000L);
    storageContext = new LongRunningStorage.LongRunningStorageContext(shardConfig);
    shard =
        new TimeSeriesShard(
            0,
            shardConfig,
            storageContext,
            encoder,
            docStore,
            memoryInfoReader,
            registry,
            LocalDateTime.now(),
            hashFunction,
            flusher,
            Executors.newSingleThreadScheduledExecutor());

    long now = System.currentTimeMillis() / 1000;
    BASE_TS = (int) ((now - (now % TimeUnit.HOURS.toSeconds(1))) - TimeUnit.HOURS.toSeconds(24));
    int ts = BASE_TS;
    while (ts < (BASE_TS + 86400)) {
      if (ts % 3600 == 0) {
        long segmentAddress = encoder.createSegment(ts);
        SEGMENT_ADDRESSES.add(segmentAddress);
        segmentTimes.add(ts);
      }
      encoder.addDataPoint(ts, 1.0);
      ts += 60;
    }

    timeSeriesRecord = timeSeriesRecordFactory.create();
    for (int i = 0; i < SEGMENT_ADDRESSES.size(); i++) {
      int segmentTime = segmentTimes.get(i);
      long segmentAddr = SEGMENT_ADDRESSES.get(i);
      if (i == 0) {
        timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddr);
      } else {
        timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddr);
      }
    }

    tsPointer = timeSeriesRecord.getAddress();
    SOURCE_FACTORY = new AuraMetricsSourceFactory();
    TIME_SERIES_STORAGE = mock(TimeSeriesStorageIf.class);
    when(TIME_SERIES_STORAGE.secondsInSegment()).thenReturn(3600);
    SOURCE_FACTORY.setTimeSeriesStorage(TIME_SERIES_STORAGE);
    when(TIME_SERIES_STORAGE.numShards()).thenReturn(1);
  }

  @BeforeMethod
  public void beforeTest() {
    node = mock(AuraMetricsQueryNode.class);
    when(node.factory()).thenReturn(SOURCE_FACTORY);
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(node.pipelineContext()).thenReturn(context);
    result = mock(AuraMetricsQueryResult.class);
    when(result.source()).thenReturn(node);
  }

  @Test
  public void fullSingleSegment() {
    setQueryTimes(BASE_TS, BASE_TS + 3600);
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS, BASE_TS + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS;
    for (; i < 60; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 60);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void fullDoubleSegment() {
    setQueryTimes(BASE_TS, BASE_TS + (3600 * 2));
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS, BASE_TS + 3600 * 2);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS;
    for (; i < 120; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 120);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void fullAllSegments() {
    setQueryTimes(BASE_TS, BASE_TS + 86400);
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS, BASE_TS + 86400);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS;
    for (; i < 60 * 24; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 60 * 24);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void startInDoubleSegment() {
    setQueryTimes(BASE_TS + 300, BASE_TS + (3600 * 2));
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS + 300, BASE_TS + (3600 * 2));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS + 300;
    for (; i < 115; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 115);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void endInDoubleSegment() {
    setQueryTimes(BASE_TS, BASE_TS + (3600 * 2) - 300);
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS, BASE_TS + (3600 * 2) - 300);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS;
    for (; i < 115; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 115);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void startAndEndInDoubleSegment() {
    setQueryTimes(BASE_TS + 300, BASE_TS + (3600 * 2) - 300);
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS, BASE_TS + (3600 * 2) - 300);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS + 300;
    for (; i < 110; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 110);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void startAndEndMidSegments() {
    setQueryTimes(BASE_TS + (3600 * 2) + 300, BASE_TS + (3600 * 5) - 300);
    long segmentTimes =
        storageContext.getSegmentTimes(BASE_TS + (3600 * 2) + 300, BASE_TS + (3600 * 5) - 300);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS + (3600 * 2) + 300;
    for (; i < 170; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 170);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void missingFirstSegment() {
    setQueryTimes(BASE_TS - 3600, BASE_TS + 3600);
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS - 3600, BASE_TS + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS;
    for (; i < 60; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 60);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void missingLastSegment() {
    setQueryTimes(BASE_TS + (3600 * 22), BASE_TS + (3600 * 23));
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS + (3600 * 22), BASE_TS + (3600 * 23));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS + (3600 * 22);
    for (; i < 60; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 60);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void missingMiddleSegmentTime() {

    // TODO: can implement a clone for TimeSeriesRecord
    TimeSeriesRecord newTSRecord = timeSeriesRecordFactory.create();
    for (int i = 0; i < SEGMENT_ADDRESSES.size(); i++) {
      int segmentTime = segmentTimes.get(i);
      long segmentAddr = SEGMENT_ADDRESSES.get(i);
      if (i == 0) {
        newTSRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddr);
      } else {
        newTSRecord.setSegmentAddress(segmentTime, segmentAddr);
      }
    }
    long timeseriesPointer = newTSRecord.getAddress();

    int segmentIndex1 = newTSRecord.getSegmentIndex(BASE_TS + (3600 * 1));
    int segmentIndex2 = newTSRecord.getSegmentIndex(BASE_TS + (3600 * 2));
    long segmentAddress2 = newTSRecord.getSegmentAddressAtIndex(segmentIndex2);
    newTSRecord.setSegmentAddressAtIndex(segmentIndex1, segmentAddress2);

    setQueryTimes(BASE_TS, BASE_TS + (3600 * 3));
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS, BASE_TS + (3600 * 3));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, timeseriesPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS;
    for (; i < 120; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      if (ts - BASE_TS == 3600) {
        ts = BASE_TS + 7200;
      }
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 1, 0.001);
    }
    assertEquals(i, 120);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void missingMiddleSegment() {
    int ts = BASE_TS;
    List<Long> segmentAddressList = new ArrayList<>();
    List<Integer> segmentTimeList = new ArrayList<>();
    while (ts < (BASE_TS + (3600 * 4))) {
      if (ts % 3600 == 0) {
        if (ts == BASE_TS + 3600) {
          ts += 3600;
        }

        long segmentAddress = encoder.createSegment(ts);
        segmentAddressList.add(segmentAddress);
        segmentTimeList.add(ts);
      }
      encoder.addDataPoint(ts, 2.5);
      ts += 60;
    }

    TimeSeriesRecord timeSeriesRecord = timeSeriesRecordFactory.create();
    for (int i = 0; i < segmentAddressList.size(); i++) {
      int segmentTime = segmentTimeList.get(i);
      long segmentAddr = segmentAddressList.get(i);
      if (i == 0) {
        timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddr);
      } else {
        timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddr);
      }
    }
    long timeseriesPointer = timeSeriesRecord.getAddress();

    setQueryTimes(BASE_TS, BASE_TS + (3600 * 3));
    int firstSegmentTime = segmentTimeList.get(0);
    int segmentCount = segmentTimeList.size();
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, timeseriesPointer, firstSegmentTime, segmentCount);
    int i = 0;
    ts = BASE_TS;
    for (; i < 120; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      if (ts - BASE_TS == 3600) {
        ts = BASE_TS + 7200;
      }
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 2.5, 0.001);
    }
    assertEquals(i, 120);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void missingMiddleSegments() {
    int ts = BASE_TS;
    List<Long> segmentAddressList = new ArrayList<>();
    List<Integer> segmentTimeList = new ArrayList<>();
    while (ts < (BASE_TS + (3600 * 4))) {
      if (ts % 3600 == 0) {
        if (ts == BASE_TS + 3600) {
          ts += (3600 * 2);
        }

        long segmentAddress = encoder.createSegment(ts);
        segmentAddressList.add(segmentAddress);
        segmentTimeList.add(ts);
      }
      encoder.addDataPoint(ts, 3.75);
      ts += 60;
    }

    TimeSeriesRecord timeSeriesRecord = timeSeriesRecordFactory.create();
    for (int i = 0; i < segmentAddressList.size(); i++) {
      int segmentTime = segmentTimeList.get(i);
      long segmentAddr = segmentAddressList.get(i);
      if (i == 0) {
        timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddr);
      } else {
        timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddr);
      }
    }
    long timeseriesPointer = timeSeriesRecord.getAddress();

    setQueryTimes(BASE_TS, BASE_TS + (3600 * 4));
    int firstSegmentTime = segmentTimeList.get(0);
    int segmentCount = segmentTimeList.size();
    AuraMetricsNumericIterator iterator =
        new AuraMetricsNumericIterator(node, timeseriesPointer, firstSegmentTime, segmentCount);
    int i = 0;
    ts = BASE_TS;
    for (; i < 120; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      if (ts - BASE_TS == 3600) {
        ts += (3600 * 2);
      }
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), 3.75, 0.001);
    }
    assertEquals(i, 120);
    assertFalse(iterator.hasNext());
  }

  private void setQueryTimes(long start, long end) {
    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    when(query.startTime()).thenReturn(new SecondTimeStamp(start));
    when(query.endTime()).thenReturn(new SecondTimeStamp(end));
    when(node.pipelineContext().query()).thenReturn(query);
  }
}
