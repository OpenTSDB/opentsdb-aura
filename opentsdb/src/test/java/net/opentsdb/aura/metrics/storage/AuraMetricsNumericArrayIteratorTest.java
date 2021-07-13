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
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AuraMetricsNumericArrayIteratorTest {

  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static AuraMetricsQueryResult RESULT;
  private static MockTSDB TSDB;
  private static AuraMetricsQueryNode NODE;
  private static TimeSeriesShardIF shard;
  private static TimeSeriesEncoderFactory<GorillaTimeSeriesEncoder> encoderFactory;
  private static BasicTimeSeriesEncoder encoder;
  private static TimeSeriesRecordFactory timeSeriesRecordFactory;
  private static TimeSeriesRecord timeSeriesRecord;
  private static TimeseriesStorageContext storageContext;
  private static AuraMetricsSourceFactory SOURCE_FACTORY;
  private static TimeSeriesStorageIf TIME_SERIES_STORAGE;
  private static List<Long> segmentAddressList = new ArrayList<>();
  private static List<Integer> segmentTimeList = new ArrayList<>();
  private static long tsPointer;

  private int base_ts;

  @BeforeClass
  public void beforeClass() {
    System.setProperty("tmpfs.dir", new File("target/tmp/memdir").getAbsolutePath());
    NUMERIC_CONFIG =
        (NumericInterpolatorConfig)
            NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                .setDataType(NumericType.TYPE.toString())
                .build();

    RESULT = mock(AuraMetricsQueryResult.class);

    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    TSDB.registry.initialize(true);

    NODE = mock(AuraMetricsQueryNode.class);
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(NODE.pipelineContext()).thenReturn(context);

    ShardConfig shardConfig = new ShardConfig();
    shardConfig.segmentSizeHour = 1;
    shardConfig.metaStoreCapacity = 10;

    MetricRegistry registry = new MetricRegistry();
    GorillaSegmentFactory segmentFactory =
        new OffHeapGorillaSegmentFactory(shardConfig.segmentBlockSizeBytes, registry);
    encoderFactory =
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
    base_ts = (int) ((now - (now % TimeUnit.HOURS.toSeconds(1))) - TimeUnit.HOURS.toSeconds(24));
    int ts = base_ts;
    while (ts < (base_ts + 86400)) {
      if (ts % 3600 == 0) {
        long segmentAddress = encoder.createSegment(ts);
        segmentAddressList.add(segmentAddress);
        segmentTimeList.add(ts);
      }
      encoder.addDataPoint(ts, 1.0);
      ts += 60;
    }

    timeSeriesRecord = timeSeriesRecordFactory.create();
    for (int i = 0; i < segmentAddressList.size(); i++) {
      int segmentTime = segmentTimeList.get(i);
      long segmentAddr = segmentAddressList.get(i);
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
    when(NODE.factory()).thenReturn(SOURCE_FACTORY);
  }

  @Test
  public void alignedQuery60sNoDupes1Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    int i = 0;
    for (; i < 60; i++) {
      assertEquals(v.value().doubleArray()[i], 1.0, 0.001);
    }
    assertEquals(60, i);
  }

  @Test
  public void testMissingDataPointIsReturnedAsNaN() {
    int start = base_ts;
    int end = start + 3600;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .setStart(String.valueOf(start))
            .setEnd(String.valueOf(end))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());

    long segmentAddress = encoder.createSegment(base_ts);
    encoder.addDataPoint(start, 1.0);
    encoder.addDataPoint(start + 60, 1.0);
    encoder.addDataPoint(start + 180, 1.5); // 3rd data point is skipped
    encoder.addDataPoint(start + 240, 1.0);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);
    long segmentTimes = storageContext.getSegmentTimes(start, end);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    double[] values = v.value().doubleArray();
    System.out.println(Arrays.toString(values));
    assertEquals(values[0], 1.0);
    assertEquals(values[1], 1.0);
    assertEquals(values[2], Double.NaN);
    assertEquals(values[3], 1.5);
    assertEquals(values[4], 1.0);
    assertEquals(values[5], Double.NaN);
  }

  @Test
  public void alignedQuery60sNoDupes2Hours() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 7200))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(120).build());
    long segmentInfo = storageContext.getSegmentTimes(base_ts, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentInfo);
    int segmentCount = storageContext.getSegmentCount(segmentInfo);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 120);
    for (int i = 0; i < 120; i++) {
      assertEquals(v.value().doubleArray()[i], 1.0, 0.001);
    }
  }

  @Test
  public void alignedQuery60sNoDupes24Hours() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + TimeUnit.HOURS.toSeconds(24)))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1440).build());
    long segmentTimes =
        storageContext.getSegmentTimes(base_ts, (int) (base_ts + TimeUnit.HOURS.toSeconds(24)));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60 * 24);
    for (int i = 0; i < 60 * 24; i++) {
      assertEquals(v.value().doubleArray()[i], 1.0, 0.001);
    }
  }

  @Test
  public void alignedQueryRunAllNoDupes1Hour() {

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setRunAll(true)
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 1);
    assertEquals(v.value().doubleArray()[0], 60, 0.001);
  }

  @Test
  public void alignedQueryRunAllNoDupes2Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setRunAll(true)
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 7200))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 1);
    assertEquals(v.value().doubleArray()[0], 120, 0.001);
  }

  @Test
  public void alignedQuery300sNoDupes1Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("5m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(12).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 12);
    for (int i = 0; i < 12; i++) {
      assertEquals(v.value().doubleArray()[i], 5.0, 0.001);
    }
  }

  @Test
  public void alignedQuery300sNoDupes2Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("5m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 7200))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(24).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 24);
    for (int i = 0; i < 24; i++) {
      assertEquals(v.value().doubleArray()[i], 5.0, 0.001);
    }
  }

  @Test
  public void alignedQuery3600sNoDupes1Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("60m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 1);
    assertEquals(v.value().doubleArray()[0], 60.0, 0.001);
  }

  @Test
  public void alignedQuery3600sNoDupes2Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("60m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 7200))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(2).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 2);
    for (int i = 0; i < 2; i++) {
      assertEquals(v.value().doubleArray()[i], 60.0, 0.001);
    }
  }

  @Test
  public void unalignedQuery60sNoDupes1Hours() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts + 900))
            .setEnd(String.valueOf(base_ts + 2700))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(30).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts + 900, base_ts + 2700);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(v.value().doubleArray().length, 30);
    for (int i = 0; i < 30; i++) {
      assertEquals(v.value().doubleArray()[i], 1, 0.001);
    }
  }

  @Test
  public void unalignedQuery60sNoDupes2Hours() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts + 900))
            .setEnd(String.valueOf(base_ts + 6300))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(90).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts + 900, base_ts + 6300);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 90);
    for (int i = 0; i < 90; i++) {
      assertEquals(v.value().doubleArray()[i], 1, 0.001);
    }
  }

  @Test
  public void unalignedQuery300sNoDupes1Hours() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("5m")
            .setStart(String.valueOf(base_ts + 900))
            .setEnd(String.valueOf(base_ts + 2700))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(6).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts + 900, base_ts + 2700);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 6);
    for (int i = 0; i < 6; i++) {
      assertEquals(v.value().doubleArray()[i], 5, 0.001);
    }
  }

  @Test
  public void unalignedQuery300sNoDupes2Hours() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("5m")
            .setStart(String.valueOf(base_ts + 900))
            .setEnd(String.valueOf(base_ts + 6300))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(18).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts + 900, base_ts + 6300);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 18);
    for (int i = 0; i < 18; i++) {
      assertEquals(v.value().doubleArray()[i], 5, 0.001);
    }
  }

  @Test
  public void alignedQuery60sShuffleNoDupes1Hours() {
    List<Integer> tss = new ArrayList<>();
    int ts = base_ts;
    while (ts < (base_ts + 3600)) {
      tss.add(ts);
      ts += 60;
    }

    Collections.shuffle(tss);

    long segmentAddress = encoder.createSegment(base_ts);
    for (int i = 0; i < tss.size(); i++) {
      encoder.addDataPoint(tss.get(i), 1.0);
    }
    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());

    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      assertEquals(v.value().doubleArray()[i], 1, 0.001);
    }
  }

  @Test
  public void alignedQuery60sDupes1Hours() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    encoder.openSegment(segmentAddressList.get(0));
    int segmentTime = encoder.getSegmentTime();

    BasicTimeSeriesEncoder newEncoder = encoderFactory.create();
    long segmentAddress = newEncoder.createSegment(segmentTime);

    encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));

    newEncoder.addDataPoint(base_ts, 1.0);
    newEncoder.addDataPoint(base_ts + 60, 1.0);
    newEncoder.addDataPoint(base_ts + 120, 1.0);

    // TODO: can implement a clone for TimeSeriesRecord
    TimeSeriesRecord newTSRecord = timeSeriesRecordFactory.create();
    long tsPointer = newTSRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);

    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      assertEquals(v.value().doubleArray()[i], 1, 0.001);
    }
  }

  @Test
  public void alignedQuery60sShuffleDupes1Hours() {
    List<Integer> tss = new ArrayList<>();
    int ts = base_ts;
    while (ts < (base_ts + 3600)) {
      tss.add(ts);
      ts += 60;
    }

    Collections.shuffle(tss);

    long segmentAddress = encoder.createSegment(base_ts);
    for (int i = 0; i < tss.size(); i++) {
      encoder.addDataPoint(tss.get(i), 1.0);
    }

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());

    encoder.addDataPoint(base_ts, 1.0);
    encoder.addDataPoint(base_ts + 60, 1.0);
    encoder.addDataPoint(base_ts + 120, 1.0);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);

    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      assertEquals(v.value().doubleArray()[i], 1, 0.001);
    }
  }

  @Test
  public void reportingInterval() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("5m")
            .setReportingInterval("30s")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(12).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 12);
    for (int i = 0; i < 12; i++) {
      assertEquals(v.value().doubleArray()[i], 0.5, 0.001);
    }
  }

  @Test
  public void nonGroupByAndp99Downsample() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("p99")
            .setInterval("5m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(12).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 12);
    for (int i = 0; i < 12; i++) {
      assertEquals(v.value().doubleArray()[i], 1, 0.001);
    }
  }

  @Test
  public void groupByAndp99Downsample() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("p99")
            .setInterval("5m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(12).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    NumericArrayAggregatorFactory factory =
        TSDB.getRegistry().getPlugin(NumericArrayAggregatorFactory.class, "sum");
    NumericArrayAggregator numericArrayAggregator =
        (NumericArrayAggregator)
            factory.newAggregator(
                DefaultArrayAggregatorConfig.newBuilder().setArraySize(12).build());

    assertTrue(iterator.hasNext());

    iterator.next(numericArrayAggregator);
    double[] values = numericArrayAggregator.doubleArray();
    assertEquals(values.length, 12);
    int i = 0;
    for (; i < 12; i++) {
      assertEquals(values[i], 1, 0.001);
    }
    assertEquals(12, i);
  }

  @Test
  public void unalignedQuery60sNoDupes1HoursMin() {
    List<Long> segmentAddresses = new ArrayList<>();
    List<Integer> segmentTimeList = new ArrayList<>();
    int ts = base_ts;
    while (ts < (base_ts + 86400)) {
      if (ts % 3600 == 0) {
        long segmentAddress = encoder.createSegment(ts);
        segmentAddresses.add(segmentAddress);
        segmentTimeList.add(ts);
      }
      encoder.addDataPoint(ts, 0);
      ts += 60;
    }

    long tsPointer = 0;
    for (int i = 0; i < segmentTimeList.size(); i++) {
      int segmentTime = segmentTimeList.get(i);
      long segmentAddr = segmentAddresses.get(i);
      if (i == 0) {
        tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddr);
      } else {
        timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddr);
      }
    }

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("mimmax")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts + 900))
            .setEnd(String.valueOf(base_ts + 2700))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(30).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts + 900, base_ts + 2700);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(
            config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 30);
    for (int i = 0; i < 30; i++) {
      assertEquals(v.value().doubleArray()[i], 0, 0.001);
    }
  }

  @Test
  public void outOfOrderQueryEndEarly() {
    // in this case we have a segment and the query is within the first part of
    // the segment BUT data we want may be written out of order and appears
    // AFTER a timestamp was encountered that is greater than the query end time.

    long now = System.currentTimeMillis() / 1000;
    int base_ts =
        (int) ((now - (now % TimeUnit.HOURS.toSeconds(1))) - TimeUnit.HOURS.toSeconds(24));
    int ts = base_ts;
    double val = 1.0;

    int segmentTime = ts;
    long segmentAddress = encoder.createSegment(ts);
    for (int i = 0; i < 3; i++) {
      encoder.addDataPoint(ts, val++);
      ts += 300;
    }
    int resume = ts;
    double valResume = val;
    ts += 300 + 300; // skip 2
    val += 2;
    for (int i = 0; i < 2; i++) {
      encoder.addDataPoint(ts, val++);
      ts += 300;
    }
    // back fill
    ts = resume;
    for (int i = 0; i < 2; i++) {
      encoder.addDataPoint(ts, valResume++);
      ts += 300;
    }

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("5m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + (300 * 5)))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(18).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 6300);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 5);
    val = 1;
    for (int i = 0; i < v.value().doubleArray().length; i++) {
      assertEquals(v.value().doubleArray()[i], val++, 0.001);
    }
  }

  @Test
  public void outOfOrderNextSegment() {
    // In this test we have two segments where the query overlaps them. The first
    // segment will contain data we need and the next segment will have data out
    // of order with the first timestamp in the segment being greater than the
    // end timestamp of the query and the next data point being within the
    // query range.

    long now = System.currentTimeMillis() / 1000;
    int base_ts =
        (int) ((now - (now % TimeUnit.HOURS.toSeconds(1))) - TimeUnit.HOURS.toSeconds(24));

    int last2hts = base_ts - (3600 * 1);
    long segmentAddressA = encoder.createSegment(last2hts);
    int segmentTimeA = last2hts;

    int ts = base_ts - 1200;
    encoder.addDataPoint(ts, 1);
    ts += 300;
    encoder.addDataPoint(ts, 2);
    ts += 300;
    encoder.addDataPoint(ts, 3);
    ts += 300;
    encoder.addDataPoint(ts, 4);

    ts = base_ts;
    long segmentAddressB = encoder.createSegment(base_ts);
    int segmentTimeB = base_ts;
    ts += 300; // later first
    encoder.addDataPoint(ts, 6);
    ts -= 300; // OOO
    encoder.addDataPoint(ts, 5);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTimeA, segmentAddressA);
    timeSeriesRecord.setSegmentAddress(segmentTimeB, segmentAddressB);

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("5m")
            .setStart(String.valueOf(base_ts - 1200))
            .setEnd(String.valueOf(base_ts + 300))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(18).build());

    int firstSegmentTime = segmentTimeA;
    int segmentCount = 2;
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 5);
    assertArrayEquals(new double[] {1, 2, 3, 4, 5}, v.value().doubleArray(), 0.001);
  }

  @Test
  public void skipIntervalForReducedRetentionOfStorage() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts + 60))
            .setEnd(String.valueOf(base_ts + 7200))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(120).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts + 3600, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 119);
    for (int i = 0; i < 59; i++) {
      assertTrue(Double.isNaN(v.value().doubleArray()[i]));
    }
    for (int i = 59; i < 119; i++) {
      assertEquals(v.value().doubleArray()[i], 1.0, 0.001);
    }
  }
}
