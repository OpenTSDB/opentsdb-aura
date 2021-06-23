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
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AuraMetricsTimeSeriesTest {
  private static int BASE_TS = 1514768400;
  private static List<long[]> SEGMENTS;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static MockTSDB TSDB;
  private static AuraMetricsSourceFactory SOURCE_FACTORY;
  private static TimeSeriesStorageIf TIME_SERIES_STORAGE;
  private static TimeseriesStorageContext storageContext;

  private AuraMetricsQueryResult result;
  private AuraMetricsQueryNode node;

  private static int firstSegmentTime;
  private static int segmentCount;

  private static TimeSeriesShardIF shard;
  private static TimeSeriesEncoderFactory encoderFactory;
  private static TimeSeriesEncoder encoder;
  private static TimeSeriesRecordFactory timeSeriesRecordFactory;
  private static TimeSeriesRecord timeSeriesRecord;
  private static List<Long> segmentAddressList = new ArrayList<>();
  private static List<Integer> segmentTimeList = new ArrayList<>();
  private static long tsPointer;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("tmpfs.dir", new File("target/tmp/memdir").getAbsolutePath());

    NUMERIC_CONFIG =
            (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
                    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                    .setDataType(NumericType.TYPE.toString())
                    .build();

    SEGMENTS = new ArrayList<>();

    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    TSDB.registry.initialize(true);

    ShardConfig shardConfig = new ShardConfig();
    shardConfig.timeSeriesTableSize = 1;
    shardConfig.tagTableSize = 1;
    shardConfig.segmentSizeHour = 1;
    shardConfig.metaStoreCapacity = 10;

    MetricRegistry registry = new MetricRegistry();
    GorillaSegmentFactory segmentFactory = new OffHeapGorillaSegmentFactory(shardConfig.segmentBlockSizeBytes, registry);
    encoderFactory =
        new GorillaTimeSeriesEncoderFactory(false, shardConfig.garbageQSize, shardConfig.segmentCollectionDelayMinutes, registry, segmentFactory);

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
    NewDocStore docStore = new NewDocStore(
            shardConfig.metaStoreCapacity, shardConfig.metaPurgeBatchSize, null, hashFunction, mock(SharedMetaResult.class), false);
    Flusher flusher = Mockito.mock(Flusher.class);
    Mockito.when(flusher.frequency()).thenReturn(30_000L);
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

    int ts = BASE_TS;
    while (ts < (BASE_TS + 3600)) {
      if (ts % 3600 == 0) {
        long segmentAddress = encoder.createSegment(ts);
        segmentTimeList.add(ts);
        segmentAddressList.add(segmentAddress);
      }
      encoder.addDataPoint(ts, 1.0);
      ts += 60;
    }

    for (int i = 0; i < segmentAddressList.size(); i++) {
      int segmentTime = segmentTimeList.get(i);
      long segmentAddress = segmentAddressList.get(i);
      if (i == 0) {
        timeSeriesRecord = timeSeriesRecordFactory.create();
        tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);
      } else {
        timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddress);
      }
    }

//    segmentTimes = segmentTimeList.stream().mapToInt(i -> i).toArray();
    firstSegmentTime = segmentTimeList.get(0);
    segmentCount = segmentTimeList.size();
    SOURCE_FACTORY = new AuraMetricsSourceFactory();
    TIME_SERIES_STORAGE = mock(TimeSeriesStorageIf.class);
    when(TIME_SERIES_STORAGE.secondsInSegment()).thenReturn(3600);
    SOURCE_FACTORY.setTimeSeriesStorage(TIME_SERIES_STORAGE);
  }

  @BeforeMethod
  public void beforeTest() {
    node = mock(AuraMetricsQueryNode.class);
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(node.pipelineContext()).thenReturn(context);
    when(node.factory()).thenReturn(SOURCE_FACTORY);
    result = mock(AuraMetricsQueryResult.class);
    when(result.source()).thenReturn(node);

    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    when(query.startTime()).thenReturn(new SecondTimeStamp(BASE_TS));
    when(query.endTime()).thenReturn(new SecondTimeStamp(BASE_TS + 3600));
    when(node.pipelineContext().query()).thenReturn(query);
  }

  @Test
  public void noPushDowns() {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
            DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                            .setMetric("sys.cpu.user").build())
                    .setId("foo")
                    .build();
    when(node.config()).thenReturn(config);

    AuraMetricsTimeSeries amts = new AuraMetricsTimeSeries();
    amts.reset(node, result, tsPointer, 0, 0, firstSegmentTime, segmentCount);
    assertEquals(amts.types().iterator().next(), NumericType.TYPE);

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            amts.iterator(NumericType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof AuraMetricsNumericIterator);

    assertTrue(amts.iterators().iterator().next() instanceof AuraMetricsNumericIterator);
  }

  @Test
  public void rate() {
    TimeSeriesDataSourceConfig config = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user").build())
            .addPushDownNode(RateConfig.newBuilder()
                    .setInterval("1s")
                    .setId("rate")
                    .build())
            .setId("foo")
            .build();
    when(node.config()).thenReturn(config);
    when(result.rateConfig()).thenReturn((RateConfig) config.getPushDownNodes().get(0));

    AuraMetricsTimeSeries amts = new AuraMetricsTimeSeries();
    amts.reset(node, result, tsPointer, 0, 0, firstSegmentTime, segmentCount);
    assertEquals(amts.types().iterator().next(), NumericType.TYPE);

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            amts.iterator(NumericType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof AuraMetricsNumericIterator);

    assertTrue(amts.iterators().iterator().next() instanceof AuraMetricsNumericIterator);
  }

  @Test
  public void downsample() {
    TimeSeriesDataSourceConfig config = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user").build())
            .addPushDownNode(DownsampleConfig.newBuilder()
                    .setAggregator("sum")
                    .setInterval("1m")
                    .setStart("2h-ago")
                    .setEnd("1h-ago")
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .setId("ds")
                    .build())
            .setId("foo")
            .build();
    when(node.config()).thenReturn(config);
    when(result.downsampleConfig()).thenReturn(
            (DownsampleConfig) config.getPushDownNodes().get(0));

    AuraMetricsTimeSeries amts = new AuraMetricsTimeSeries();
    amts.reset(node, result, tsPointer, 0, 0, firstSegmentTime, segmentCount);
    assertEquals(amts.types().iterator().next(), NumericArrayType.TYPE);

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            amts.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof AuraMetricsNumericArrayIterator);

    assertTrue(amts.iterators().iterator().next() instanceof AuraMetricsNumericArrayIterator);
  }

  @Test
  public void rateAndDownsample() {
    TimeSeriesDataSourceConfig config = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.user").build())
            .addPushDownNode(DownsampleConfig.newBuilder()
                    .setAggregator("sum")
                    .setInterval("1m")
                    .setStart("2h-ago")
                    .setEnd("1h-ago")
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .setId("ds")
                    .build())
            .addPushDownNode(RateConfig.newBuilder()
                    .setInterval("1s")
                    .setId("rate")
                    .build())
            .setId("foo")
            .build();
    when(node.config()).thenReturn(config);
    when(result.downsampleConfig()).thenReturn(
            (DownsampleConfig) config.getPushDownNodes().get(0));
    when(result.rateConfig()).thenReturn((RateConfig) config.getPushDownNodes().get(1));

    AuraMetricsTimeSeries amts = new AuraMetricsTimeSeries();
    amts.reset(node, result, tsPointer, 0, 0, firstSegmentTime, segmentCount);
    assertEquals(amts.types().iterator().next(), NumericArrayType.TYPE);

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            amts.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());
    assertTrue(op.get() instanceof AuraMetricsNumericArrayIterator);

    assertTrue(amts.iterators().iterator().next() instanceof AuraMetricsNumericArrayIterator);
  }

  // TODO - last dp iterators.
}
