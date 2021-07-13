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

import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.core.BasicTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AerospikeBatchQueryNodeTest {

  private static int SECONDS_IN_RECORD = 3600 * 6;
  private static int BATCH_LIMIT = 3;
  private static int JOBS_PER_QUERY = 3;
  private static final int BASE_TS = 1617235200;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static MockTSDB TSDB;
  private static TimeSeriesShardIF shard;
  private static TimeSeriesEncoderFactory encoderFactory;
  private static BasicTimeSeriesEncoder encoder;
  private static TimeSeriesRecordFactory timeSeriesRecordFactory;
  private static TimeSeriesRecord timeSeriesRecord;
  private static AerospikeBatchSourceFactory SOURCE_FACTORY;
  private static AerospikeClientPlugin AS_CLIENT;
  private static LTSAerospike CLIENT;

  private QueryPipelineContext context;
  private TimeSeriesDataSourceConfig config;

  @BeforeClass
  public static void beforeClass() throws Exception {
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
    CLIENT = mock(LTSAerospike.class);
    when(AS_CLIENT.asClient()).thenReturn(CLIENT);
    when(CLIENT.secondsInRecord()).thenReturn(SECONDS_IN_RECORD);

    TSDB.registry.registerPlugin(AerospikeClientPlugin.class, null, AS_CLIENT);
    SOURCE_FACTORY = new AerospikeBatchSourceFactory();
    //SOURCE_FACTORY.initialize(TSDB, null).join();
    TSDB.getConfig().register(AerospikeBatchSourceFactory.SECONDS_IN_SEGMENT_KEY, SECONDS_IN_RECORD, false, "UT");
    TSDB.getConfig().register(AerospikeBatchSourceFactory.AS_BATCH_LIMIT_KEY, BATCH_LIMIT, true, "UT");
    TSDB.getConfig().register(AerospikeBatchSourceFactory.AS_JOBS_PER_QUERY, JOBS_PER_QUERY, true, "UT");
  }

  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    config = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.busy")
                    .build())
            .setId("m1")
            .build();
    TSDB.runnables.clear();
  }

  @Test
  public void startBatches1Group1Hash() throws Exception {
    AerospikeBatchQueryNode node = new AerospikeBatchQueryNode(SOURCE_FACTORY, context, config, true);
    setupBatches(node, BASE_TS, BASE_TS + SECONDS_IN_RECORD, 1);
    CountDownLatch latch = node.startBatches();

    assertEquals(1, TSDB.runnables.size());
    assertEquals(1, latch.getCount());
    assertJobIndexes(0, 0, 0, 0, 1);
  }

  @Test
  public void startBatches1Group2Hashs() throws Exception {
    AerospikeBatchQueryNode node = new AerospikeBatchQueryNode(SOURCE_FACTORY, context, config, true);
    setupBatches(node, BASE_TS, BASE_TS + SECONDS_IN_RECORD, 2);
    CountDownLatch latch = node.startBatches();

    assertEquals(1, TSDB.runnables.size());
    assertEquals(1, latch.getCount());
    assertJobIndexes(0, 0, 0, 0, 2);
  }

  @Test
  public void startBatches1GroupManyHashes() throws Exception {
    AerospikeBatchQueryNode node = new AerospikeBatchQueryNode(SOURCE_FACTORY, context, config, true);
    setupBatches(node, BASE_TS, BASE_TS + SECONDS_IN_RECORD, 12);
    CountDownLatch latch = node.startBatches();

    assertEquals(3, TSDB.runnables.size());
    assertEquals(3, latch.getCount());
    assertJobIndexes(0, 0, 0, 0, 4);
    assertJobIndexes(1, 0, 0, 4, 8);
    assertJobIndexes(2, 0, 0, 8, 12);
  }

  @Test
  public void startBatches2Groups2Hashes() throws Exception {
    AerospikeBatchQueryNode node = new AerospikeBatchQueryNode(SOURCE_FACTORY, context, config, true);
    setupBatches(node, BASE_TS, BASE_TS + SECONDS_IN_RECORD, 2, 2);
    CountDownLatch latch = node.startBatches();

    assertEquals(2, TSDB.runnables.size());
    assertEquals(2, latch.getCount());
    assertJobIndexes(0, 0, 1, 0, 1);
    assertJobIndexes(1, 1, 1, 1, 2);
  }

  @Test
  public void startBatches2Groups2HashesWide() throws Exception {
    AerospikeBatchQueryNode node = new AerospikeBatchQueryNode(SOURCE_FACTORY, context, config, true);
    setupBatches(node, BASE_TS, BASE_TS +
            (SECONDS_IN_RECORD * 24), 2, 2);
    CountDownLatch latch = node.startBatches();

    assertEquals(2, TSDB.runnables.size());
    assertEquals(2, latch.getCount());
    assertJobIndexes(0, 0, 0, 0, 2);
    assertJobIndexes(1, 1, 1, 0, 2);
  }

  @Test
  public void startBatchesBigMiddle() throws Exception {
    AerospikeBatchQueryNode node = new AerospikeBatchQueryNode(SOURCE_FACTORY, context, config, true);
    setupBatches(node, BASE_TS, BASE_TS +
            (SECONDS_IN_RECORD * 24), 2, 18, 1);
    CountDownLatch latch = node.startBatches();

    assertEquals(3, TSDB.runnables.size());
    assertEquals(3, latch.getCount());
    assertJobIndexes(0, 0, 1, 0, 5);
    assertJobIndexes(1, 1, 1, 5, 12);
    assertJobIndexes(2, 1, 2, 12, 1);
  }

  @Test
  public void startBatchesBigStart() throws Exception {
    AerospikeBatchQueryNode node = new AerospikeBatchQueryNode(SOURCE_FACTORY, context, config, true);
    setupBatches(node, BASE_TS, BASE_TS +
            (SECONDS_IN_RECORD * 24), 18, 2, 1);
    CountDownLatch latch = node.startBatches();

    assertEquals(3, TSDB.runnables.size());
    assertEquals(3, latch.getCount());
    assertJobIndexes(0, 0, 0, 0, 7);
    assertJobIndexes(1, 0, 0, 7, 14);
    assertJobIndexes(2, 0, 2, 14, 1);
  }

  @Test
  public void startBatchesBigEnd() throws Exception {
    AerospikeBatchQueryNode node = new AerospikeBatchQueryNode(SOURCE_FACTORY, context, config, true);
    setupBatches(node, BASE_TS, BASE_TS +
            (SECONDS_IN_RECORD * 24), 2, 1, 18);
    CountDownLatch latch = node.startBatches();

    assertEquals(3, TSDB.runnables.size());
    assertEquals(3, latch.getCount());
    assertJobIndexes(0, 0, 2, 0, 4);
    assertJobIndexes(1, 2, 2, 4, 11);
    assertJobIndexes(2, 2, 2, 11, 18);
  }

  void assertJobIndexes(int jobIndex, int startGroupId, int endGroupId, int startHashId, int endHashId) {
    AerospikeBatchJob job = (AerospikeBatchJob) TSDB.runnables.get(jobIndex);
    assertEquals(startGroupId, job.startGroupId);
    assertEquals(endGroupId, job.endGroupId);
    assertEquals(startHashId, job.startHashId);
    assertEquals(endHashId, job.endHashId);
  }

  void setupBatches(AerospikeBatchQueryNode node, int segmentStart, int segmentEnd, int... numHashesInGroup) {
    node.segmentsStart = segmentStart;
    node.segmentsEnd = segmentEnd;
    long hash = 0;
    DefaultMetaTimeSeriesQueryResult result = new DefaultMetaTimeSeriesQueryResult();
    for (int i = 0; i < numHashesInGroup.length; i++) {
      DefaultMetaTimeSeriesQueryResult.DefaultGroupResult gr = new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult();
      gr.addTagHash(i);
      for (int x = 0; x < numHashesInGroup[i]; x++) {
        gr.addHash(hash++);
      }
      result.addGroupResult(gr);
    }
    node.metaResult = result;
  }
}