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
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseAMNumericArrayTest {

  protected static NumericInterpolatorConfig NUMERIC_CONFIG;
  protected static AuraMetricsQueryResult RESULT;
  protected static MockTSDB TSDB;
  protected static AuraMetricsQueryNode NODE;
  protected static TimeSeriesShardIF shard;
  protected static TimeSeriesEncoderFactory<GorillaRawTimeSeriesEncoder> encoderFactory;
  protected static RawTimeSeriesEncoder encoder;
  protected static TimeSeriesRecordFactory timeSeriesRecordFactory;
  protected static TimeSeriesRecord timeSeriesRecord;
  protected static TimeseriesStorageContext storageContext;
  protected static AuraMetricsSourceFactory SOURCE_FACTORY;
  protected static TimeSeriesStorageIf TIME_SERIES_STORAGE;
  protected static List<Long> segmentAddressList = new ArrayList<>();
  protected static List<Integer> segmentTimeList = new ArrayList<>();
  protected static long tsPointer;

  protected int base_ts;

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
    shardConfig.metricTableSize = 10;
    shardConfig.tagTableSize = 10;

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
    // NOTE: Call one of the setup functions.
  }

  public void setupConstantData(double value) {
    int ts = base_ts;
    while (ts < (base_ts + 86400)) {
      if (ts % 3600 == 0) {
        long segmentAddress = encoder.createSegment(ts);
        segmentAddressList.add(segmentAddress);
        segmentTimeList.add(ts);
      }
      encoder.addDataPoint(ts, value);
      ts += 60;
    }
    finishSetup();
  }

  public void setupTimestampData() {
    int ts = base_ts;
    while (ts < (base_ts + 86400)) {
      if (ts % 3600 == 0) {
        long segmentAddress = encoder.createSegment(ts);
        segmentAddressList.add(segmentAddress);
        segmentTimeList.add(ts);
      }
      encoder.addDataPoint(ts, ts);
      ts += 60;
    }
    finishSetup();
  }

  public void setupRateData() {
    int ts = base_ts;
    int cnt = 0;
    while (ts < (base_ts + 86400)) {
      if (ts % 3600 == 0) {
        long segmentAddress = encoder.createSegment(ts);
        segmentAddressList.add(segmentAddress);
        segmentTimeList.add(ts);
      }
      encoder.addDataPoint(ts, 100 * (cnt++ + 1));
      ts += 60;
    }
    finishSetup();
  }

  void finishSetup() {
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
    when(NODE.shiftSeconds()).thenReturn(0);
  }
}
