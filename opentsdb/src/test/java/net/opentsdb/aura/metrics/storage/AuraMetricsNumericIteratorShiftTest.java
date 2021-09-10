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

import io.ultrabrew.metrics.MetricRegistry;
import net.opentsdb.aura.metrics.core.Flusher;
import net.opentsdb.aura.metrics.core.LongRunningStorage;
import net.opentsdb.aura.metrics.core.MemoryInfoReader;
import net.opentsdb.aura.metrics.core.OffHeapTimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.RawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.ShardConfig;
import net.opentsdb.aura.metrics.core.StorageMode;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesShard;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.core.TimeseriesStorageContext;
import net.opentsdb.aura.metrics.core.XxHash;
import net.opentsdb.aura.metrics.core.gorilla.GorillaRawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.GorillaSegmentFactory;
import net.opentsdb.aura.metrics.core.gorilla.GorillaTimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.gorilla.OffHeapGorillaSegmentFactory;
import net.opentsdb.aura.metrics.meta.NewDocStore;
import net.opentsdb.aura.metrics.meta.SharedMetaResult;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
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

public class AuraMetricsNumericIteratorShiftTest {
  private static int BASE_TS;
  private static MockTSDB TSDB;
  private static long tsPointer;
  private static TimeSeriesShardIF shard;
  private static RawTimeSeriesEncoder encoder;
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
    TimeSeriesEncoderFactory<GorillaRawTimeSeriesEncoder> encoderFactory =
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
      encoder.addDataPoint(ts, ts);
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
  public void timeShift() throws Exception {
    setQueryTimes(BASE_TS + 3600 + 300, BASE_TS + (3600 * 3) - 300);
    when (node.shiftSeconds()).thenReturn(3600);
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS, BASE_TS + (3600 * 2));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
            new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS + 3600 + 300;
    int val = BASE_TS + 300;
    for (; i < 110; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), val, 0.001);
      val += 60;
    }
    assertEquals(i, 110);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void timeShiftOORange() throws Exception {
    setQueryTimes(BASE_TS + 3600 + 300, BASE_TS + (3600 * 3) - 300);
    when (node.shiftSeconds()).thenReturn(3600 * 2);
    long segmentTimes = storageContext.getSegmentTimes(BASE_TS, BASE_TS + (3600 * 2));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericIterator iterator =
            new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount);
    int i = 0;
    int ts = BASE_TS + (3600 * 2);
    int val = BASE_TS;
    for (; i < 55; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(v.timestamp().epoch(), ts);
      ts += 60;
      assertFalse(v.value().isInteger());
      assertEquals(v.value().doubleValue(), val, 0.001);
      val += 60;
    }
    assertEquals(i, 55);
    assertFalse(iterator.hasNext());
  }

  private void setQueryTimes(long start, long end) {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
            DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                            .setMetric("sys.cpu.user").build())
                    .setStartTimeStamp(new SecondTimeStamp(start))
                    .setEndTimeStamp(new SecondTimeStamp(end))
                    .setId("foo")
                    .build();
    when(node.config()).thenReturn(config);
  }
}
