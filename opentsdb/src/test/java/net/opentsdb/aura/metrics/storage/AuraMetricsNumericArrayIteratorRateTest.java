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
import net.opentsdb.aura.metrics.core.TimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.core.TimeSeriesShard;
import net.opentsdb.aura.metrics.core.TimeseriesStorageContext;
import net.opentsdb.aura.metrics.core.XxHash;
import net.opentsdb.aura.metrics.core.gorilla.GorillaSegmentFactory;
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
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.utils.Pair;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AuraMetricsNumericArrayIteratorRateTest {

  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static AuraMetricsQueryResult RESULT;
  private static MockTSDB TSDB;
  private static AuraMetricsQueryNode NODE;
  private static TimeSeriesShardIF shard;
  private static TimeSeriesEncoderFactory encoderFactory;
  private static TimeSeriesEncoder encoder;
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
    int idx = 0;
    int cnt = 0;
    while (ts < (base_ts + 86400)) {
      if (ts % 3600 == 0) {
        long segmentAddress = encoder.createSegment(ts);
        segmentAddressList.add(segmentAddress);
        segmentTimeList.add(ts);
      }
      encoder.addDataPoint(ts, 100.0 * (cnt++ + 1));
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
  public void alignedQunery60sNoDupes1Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
    }
  }

  @Test
  public void alignedQunery60sNoDupes1HourAsCount() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder().setInterval("1s").setRateToCount(true).setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], (100 * (i + 1) * 60), 0.001);
      }
    }
  }

  @Test
  public void alignedQunery60sNoDupes1HourAsCountWithInterval() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder()
            .setInterval("1s")
            .setInterval("10s")
            .setRateToCount(true)
            .setId("rate")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], (10 * (i + 1) * 60), 0.001);
      }
    }
  }

  @Test
  public void alignedQunery60sNoDupes1HourAsCount60s() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder().setInterval("60s").setRateToCount(true).setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], (100 * (i + 1)), 0.001);
      }
    }
  }

  @Test
  public void alignedQuery60sNoDupes2Hours120sRate() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 7200))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        (RateConfig) RateConfig.newBuilder().setInterval("120s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(120).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 120);
    for (int i = 0; i < 120; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], 200, 0.001);
      }
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

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1440).build());
    long segmentTimes =
        storageContext.getSegmentTimes(base_ts, (int) (base_ts + TimeUnit.HOURS.toSeconds(24)));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60 * 24);
    for (int i = 0; i < 60 * 24; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
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

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 1);
    assertEquals(v.value().doubleArray()[0], 98.333, 0.001);
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

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 1);
    assertEquals(v.value().doubleArray()[0], 198.333, 0.001);
  }

  @Test
  public void alignedQuery60sShuffleNoDupes1Hours() {
    List<Pair<Integer, Double>> tss = new ArrayList<>();
    int ts = base_ts;
    int count = 0;
    while (ts < (base_ts + 3600)) {
      tss.add(new Pair<>(ts, (double) 100 * (count++ + 1)));
      ts += 60;
    }

    Collections.shuffle(tss);
    long segmentAddress = encoder.createSegment(base_ts);
    for (int i = 0; i < tss.size(); i++) {
      encoder.addDataPoint(tss.get(i).getKey(), tss.get(i).getValue());
    }

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();

    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN((v.value().doubleArray()[i])));
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
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

    RateConfig rate =
        RateConfig.newBuilder()
            .setInterval("1s")
            .setCounter(true)
            .setDropResets(true)
            .setId("rate")
            .build();

    TimeSeriesEncoder newEncoder = encoderFactory.create();
    int segmentTime = segmentTimeList.get(0);
    long segmentAddress = newEncoder.createSegment(segmentTime);

    encoder.openSegment(segmentAddressList.get(0));
    encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));

    newEncoder.addDataPoint(base_ts, 100000.0);
    newEncoder.addDataPoint(base_ts + 60, 1100000.0);
    newEncoder.addDataPoint(base_ts + 120, 1100000.0);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);

    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0 || i == 3) {
        assertTrue(Double.isNaN((v.value().doubleArray()[i])));
      } else if (i == 1) {
        assertEquals(v.value().doubleArray()[i], 16666.666, 0.001);
      } else if (i == 2) {
        assertEquals(v.value().doubleArray()[i], 0.00, 0.001);
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
    }
  }

  @Test
  public void alignedQuery60sShuffleDupes1HoursCounter() {
    List<Pair<Integer, Double>> tss = new ArrayList<>();
    int ts = base_ts;
    int count = 0;
    while (ts < (base_ts + 3600)) {
      tss.add(new Pair<>(ts, (double) 100 * (count++ + 1)));
      ts += 60;
    }

    Collections.shuffle(tss);
    long segmentAddress = encoder.createSegment(base_ts);
    for (int i = 0; i < tss.size(); i++) {
      encoder.addDataPoint(tss.get(i).getKey(), tss.get(i).getValue());
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

    RateConfig rate =
        RateConfig.newBuilder()
            .setInterval("1s")
            .setCounter(true)
            .setDropResets(true)
            .setId("rate")
            .build();

    // we always take latest here.
    encoder.addDataPoint(base_ts, 100000.0);
    encoder.addDataPoint(base_ts + 60, 100000.0);
    encoder.addDataPoint(base_ts + 120, 100000.0);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0 || i == 3) {
        assertTrue(Double.isNaN((v.value().doubleArray()[i])));
      } else if (i == 1 || i == 2) {
        assertEquals(v.value().doubleArray()[i], 0.00, 0.001);
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
    }
  }

  @Test
  public void rate15mDownsample() {
    long segmentAddress = encoder.createSegment(base_ts);
    int ts = base_ts;
    int cnt = 0;
    for (int i = 0; i < 60; i++) {
      encoder.addDataPoint(ts, 100.0 * (cnt++ + 1));
      ts += 60;
    }

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("15m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder()
            .setInterval("1s")
            .setCounter(true)
            .setDropResets(true)
            .setId("rate")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(4).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 4);
    for (int i = 0; i < 4; i++) {
      if (i == 0) {
        assertEquals(v.value().doubleArray()[i], 23.333, 0.001);
      } else {
        assertEquals(v.value().doubleArray()[i], 25.000, 0.001);
      }
    }
  }
}
