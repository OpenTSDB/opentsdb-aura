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

import net.opentsdb.aura.metrics.core.BasicTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.File;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class AerospikeQueryNodeTest {

  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static MockTSDB TSDB;
  private static TimeSeriesShardIF shard;
  private static TimeSeriesEncoderFactory encoderFactory;
  private static BasicTimeSeriesEncoder encoder;
  private static TimeSeriesRecordFactory timeSeriesRecordFactory;
  private static TimeSeriesRecord timeSeriesRecord;
  private static AerospikeSourceFactory SOURCE_FACTORY;
  private static AerospikeClientPlugin AS_CLIENT;

  private QueryPipelineContext context;

  @BeforeClass
  public void beforeClass() {
    System.setProperty("tmpfs.dir", new File("target/tmp/memdir").getAbsolutePath());
    NUMERIC_CONFIG =
            (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
                    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                    .setRealFillPolicy(QueryFillPolicy.FillWithRealPolicy.PREFER_NEXT)
                    .setDataType(NumericType.TYPE.toString())
                    .build();

    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    TSDB.registry.initialize(true);


    AS_CLIENT = mock(AerospikeClientPlugin.class);
    TSDB.registry.registerPlugin(AerospikeClientPlugin.class, null, AS_CLIENT);
//    ShardConfig shardConfig = new ShardConfig();
//    shardConfig.segmentSizeHour = 1;
//    shardConfig.metaStoreCapacity = 10;
//
//    MetricRegistry registry = new MetricRegistry();
//    GorillaSegmentFactory segmentFactory = new OffHeapGorillaSegmentFactory(shardConfig.segmentBlockSizeBytes, registry);
//    encoderFactory =
//            new GorillaTimeSeriesEncoderFactory(false, shardConfig.garbageQSize, shardConfig.segmentCollectionDelayMinutes, registry, segmentFactory);
//
//    int segmentsInATimeSeries =
//            getSegmentsInATimeSeries(shardConfig.retentionHour, shardConfig.segmentSizeHour);
//    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(1);
//    int secondsInATimeSeries = secondsInASegment * segmentsInATimeSeries;
//
//    timeSeriesRecordFactory =
//            new OffHeadTimeSeriesRecordFactory(
//                    segmentsInATimeSeries, secondsInASegment, secondsInATimeSeries);
//
//    AuraMetricsNumericArrayIterator.timeSeriesEncoderFactory = encoderFactory;
//    AuraMetricsNumericArrayIterator.timeSeriesRecordFactory = timeSeriesRecordFactory;
//
//    encoder = encoderFactory.create();
//    MemoryInfoReader memoryInfoReader = mock(MemoryInfoReader.class);
//    HashFunction hashFunction = new XxHash();
//    NewDocStore docStore = new NewDocStore(
//            shardConfig.metaStoreCapacity, shardConfig.metaPurgeBatchSize, null, hashFunction, mock(SharedMetaResult.class), false);
//    Flusher flusher = mock(Flusher.class);
//    when(flusher.frequency()).thenReturn(30_000L);
//    shard =
//            new net.opentsdb.aura.metrics.core.TimeSeriesShard(
//                    0,
//                    encoder,
//                    shardConfig,
//                    docStore,
//                    memoryInfoReader,
//                    registry,
//                    Executors.newSingleThreadScheduledExecutor(),
//                    LocalDateTime.now(),
//                    hashFunction,
//                    flusher);
//
//    long now = System.currentTimeMillis() / 1000;
//    base_ts = (int) ((now - (now % TimeUnit.HOURS.toSeconds(1))) - TimeUnit.HOURS.toSeconds(24));
//    int ts = base_ts;
//    while (ts < (base_ts + 86400)) {
//      if (ts % 3600 == 0) {
//        long segmentAddress = encoder.createSegment(ts);
//        segmentAddressList.add(segmentAddress);
//        segmentTimeList.add(ts);
//      }
//      encoder.addDataPoint(ts, 1.0);
//      ts += 60;
//    }
//
//    timeSeriesRecord = timeSeriesRecordFactory.create();
//    for (int i = 0; i < segmentAddressList.size(); i++) {
//      int segmentTime = segmentTimeList.get(i);
//      long segmentAddr = segmentAddressList.get(i);
//      if (i == 0) {
//        timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddr);
//      } else {
//        timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddr);
//      }
//    }
//    tsPointer = timeSeriesRecord.getAddress();
    SOURCE_FACTORY = new AerospikeSourceFactory();
//    TIME_SERIES_STORAGE = mock(TimeSeriesStorageIf.class);
//    when(TIME_SERIES_STORAGE.secondsInSegment()).thenReturn(3600);
//    SOURCE_FACTORY.setTimeSeriesStorage(TIME_SERIES_STORAGE);
//    when(TIME_SERIES_STORAGE.numShards()).thenReturn(1);
//    when(NODE.factory()).thenReturn(SOURCE_FACTORY);
  }

  @BeforeMethod
  public void before() {
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
  }

//  @Test
//  public void timeStamps() throws Exception {
//    DefaultTimeSeriesDataSourceConfig config = DefaultTimeSeriesDataSourceConfig.newBuilder()
//            .setMetric(MetricLiteralFilter.newBuilder()
//                    .setMetric("sys.cpu.usr")
//                    .build())
//            .setId("m1")
//            .build();
//    AerospikeQueryNode node = new AerospikeQueryNode(SOURCE_FACTORY,
//            context, config, false);
//
//    SemanticQuery query =
//            SemanticQuery.newBuilder()
//                    .setMode(QueryMode.SINGLE)
//                    .addExecutionGraphNode(config)
//                    .setStart("1614556800")
//                    .setEnd("1614578400")
//                    .build();
//    when(context.query()).thenReturn(query);
//    node.initialize(null).join();
//    assertEquals(node.getSegmentsStart(), 1614556800);
//    assertEquals(node.getSegmentsEnd(), 1614556800 + (3600 * 6));
//
//    // shift
//    query = SemanticQuery.newBuilder()
//            .setMode(QueryMode.SINGLE)
//            .addExecutionGraphNode(config)
//            .setStart("1614558805")
//            .setEnd("1614580405")
//            .build();
//    when(context.query()).thenReturn(query);
//    node.initialize(null).join();
//    assertEquals(node.getSegmentsStart(), 1614556800);
//    assertEquals(node.getSegmentsEnd(), 1614556800 + (3600 * 8));
//
//    // short one
//    query = SemanticQuery.newBuilder()
//            .setMode(QueryMode.SINGLE)
//            .addExecutionGraphNode(config)
//            .setStart("1614558805")
//            .setEnd("1614559105")
//            .build();
//    when(context.query()).thenReturn(query);
//    node.initialize(null).join();
//    assertEquals(node.getSegmentsStart(), 1614556800);
//    assertEquals(node.getSegmentsEnd(), 1614556800 + (3600 * 2));
//  }

}