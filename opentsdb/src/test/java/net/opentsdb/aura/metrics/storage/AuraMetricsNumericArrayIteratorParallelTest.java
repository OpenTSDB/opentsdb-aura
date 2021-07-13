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
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.ArrayAverageFactory;
import net.opentsdb.data.types.numeric.aggregators.ArraySumFactory;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AuraMetricsNumericArrayIteratorParallelTest {

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
  private int base;
  private static ArraySumFactory FACTORY;

  @BeforeClass
  public void beforeClass() {
    System.setProperty("tmpfs.dir", new File("target/tmp/memdir").getAbsolutePath());

    NUMERIC_CONFIG =
        (NumericInterpolatorConfig)
            NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                .setRealFillPolicy(QueryFillPolicy.FillWithRealPolicy.PREFER_NEXT)
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
            mock(ScheduledExecutorService.class));

    long now = System.currentTimeMillis() / 1000;
    base = (int) (now - (now % TimeUnit.HOURS.toSeconds(2)));

    long segmentAddress = encoder.createSegment(base);
    segmentAddressList.add(segmentAddress);
    segmentTimeList.add(base);
    double value = 5.0;
    for (int i = 0; i < 60; i++) {
      encoder.addDataPoint((base + (60 * i)), value);
    }

    timeSeriesRecord = timeSeriesRecordFactory.create();
    tsPointer =
        AuraMetricsNumericArrayIteratorParallelTest.timeSeriesRecord.create(
            0, 0, (byte) 0, 0, 0.0, base, segmentAddress);

    FACTORY = new ArraySumFactory();
    SOURCE_FACTORY = new AuraMetricsSourceFactory();
    TIME_SERIES_STORAGE = mock(TimeSeriesStorageIf.class);
    when(TIME_SERIES_STORAGE.secondsInSegment()).thenReturn(3600);
    SOURCE_FACTORY.setTimeSeriesStorage(TIME_SERIES_STORAGE);
    when(TIME_SERIES_STORAGE.numShards()).thenReturn(1);
    when(NODE.factory()).thenReturn(SOURCE_FACTORY);
  }

  @Test
  public void testDownSample() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval + "m")
            .setStart(String.valueOf(base))
            .setEnd(String.valueOf(base + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    long segmentTimes = storageContext.getSegmentTimes(base, base + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[60 / interval];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();
    assertEquals(
        downSampled,
        new double[] {25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0});
  }

  @Test
  public void testDownSampleIntervalNotAlignedToSegment() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval(interval + "m")
            .setStart(String.valueOf((base + 322)))
            .setEnd(String.valueOf((base + (3600 - 685))))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    long segmentTimes = storageContext.getSegmentTimes(base + 322, base + (3600 - 685));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[config.intervals()];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();
    assertEquals(downSampled, new double[] {5, 5, 5.0, 5.0, 5.0, 5.0, 5.0});
  }

  @Test
  public void testSkipSegmentsBeforeTheStartTime() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval + "m")
            .setStart(String.valueOf(base))
            .setEnd(String.valueOf(base + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    List<Integer> segTimes = new ArrayList<>();
    List<Long> segAddresses = new ArrayList<>();

    int segmentTime = base - (3600 * 2);
    segTimes.add(segmentTime);
    long segmentAddress = encoder.createSegment(segmentTime);
    segAddresses.add(segmentAddress);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);

    segmentTime = base - 3600;
    segTimes.add(segmentTime);
    segmentAddress = encoder.createSegment(segmentTime);
    segAddresses.add(segmentAddress);

    timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddress);

    BasicTimeSeriesEncoder newEncoder = encoderFactory.create();
    for (int i = 0; i < segmentAddressList.size(); i++) {
      encoder.openSegment(segmentAddressList.get(i));
      int segTime = segmentTimeList.get(i);
      long segAddress = newEncoder.createSegment(segTime);
      encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));
      timeSeriesRecord.setSegmentAddress(segTime, segAddress);
      segTimes.add(segTime);
      segAddresses.add(segAddress);
    }

    long segmentTimes = storageContext.getSegmentTimes(base, base + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(
            config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[60 / interval];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();
    assertEquals(
        downSampled,
        new double[] {25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0});
  }

  @Test
  public void testSkipSegmentsAfterTheEndTime() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval + "m")
            .setStart(String.valueOf(base))
            .setEnd(String.valueOf(base + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    List<Integer> segmentTimeList = new ArrayList<>();
    List<Long> segmentAddresses = new ArrayList<>();
    long tsPointer = 0;
    BasicTimeSeriesEncoder newEncoder = encoderFactory.create();
    for (int i = 0; i < AuraMetricsNumericArrayIteratorParallelTest.segmentTimeList.size(); i++) {
      encoder.openSegment(segmentAddressList.get(i));

      int segmentTime = AuraMetricsNumericArrayIteratorParallelTest.segmentTimeList.get(i);
      long segmentAddress = newEncoder.createSegment(segmentTime);
      segmentAddresses.add(segmentAddress);
      segmentTimeList.add(segmentTime);
      encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));

      if (i == 0) {
        tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);
      } else {
        timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddress);
      }
    }

    int segmentTime = base + 3600;
    segmentTimeList.add(segmentTime);
    long segmentAddress = encoder.createSegment(segmentTime);
    segmentAddresses.add(segmentAddress);

    segmentTime = base + (3600 * 2);
    segmentTimeList.add(segmentTime);
    segmentAddress = encoder.createSegment(segmentTime);
    segmentAddresses.add(segmentAddress);

    long segmentTimes = storageContext.getSegmentTimes(base, base + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(
            config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[60 / interval];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();
    assertEquals(
        downSampled,
        new double[] {25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0});
  }

  @Test
  public void testSkipIntervalsBeforeTheStartTime() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval + "m")
            .setStart(String.valueOf(base - 1800))
            .setEnd(String.valueOf(base + 1800))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    List<Integer> segmentTimeList = new ArrayList<>();
    List<Long> segmentAddresses = new ArrayList<>();
    int segmentTime = base - 3600;
    segmentTimeList.add(segmentTime);
    long segmentAddress = encoder.createSegment(segmentTime);
    segmentAddresses.add(segmentAddress);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);

    BasicTimeSeriesEncoder newEncoder = encoderFactory.create();
    for (int i = 0; i < segmentAddressList.size(); i++) {
      int segTime = AuraMetricsNumericArrayIteratorParallelTest.segmentTimeList.get(i);
      encoder.openSegment(segmentAddressList.get(i));
      long segAddress = newEncoder.createSegment(segTime);
      encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));
      timeSeriesRecord.setSegmentAddress(segTime, segAddress);
    }

    long segmentTimes = storageContext.getSegmentTimes(base - 1800, base + 1800);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(
            config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[60 / interval];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();

    int i = 0;
    for (; i < 6; i++) {
      assertTrue(Double.isNaN(downSampled[i]));
    }
    for (; i < 12; i++) {
      assertEquals(downSampled[i], 25.0);
    }
  }

  @Test
  public void testDownSampleAvgGroupByAvg() {
    long segmentAddress = encoder.createSegment(base);

    double[] values = new double[] {1.0, 5.0, 10.0};
    encoder.addDataPoint(base, values[0]);

    for (int i = 1; i < 360; i++) {
      encoder.addDataPoint((base + (10 * i)), values[i % 3]);
    }

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base, segmentAddress);

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .setStart(String.valueOf(base))
            .setEnd(String.valueOf(base + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    long segmentTimes = storageContext.getSegmentTimes(base, base + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArrayAverageFactory.ArrayAverage dsAggregator =
        new ArrayAverageFactory.ArrayAverage(false, new ArrayAverageFactory());
    double[] nans = new double[60];
    java.util.Arrays.fill(nans, Double.NaN);
    dsAggregator.accumulate(nans);

    iterator.next(dsAggregator);

    ArrayAverageFactory.ArrayAverage groupByAggregator =
        new ArrayAverageFactory.ArrayAverage(false, new ArrayAverageFactory());
    groupByAggregator.combine(dsAggregator);
    groupByAggregator.combine(dsAggregator);

    double[] result = groupByAggregator.doubleArray();
    assertEquals(result.length, 60);
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], 5.333333333333333);
    }
  }
}
