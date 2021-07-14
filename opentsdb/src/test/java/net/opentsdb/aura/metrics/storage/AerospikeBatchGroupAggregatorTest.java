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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.core.TSDataConsumer;
import net.opentsdb.aura.metrics.core.RawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.ArraySumFactory;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;
import static org.testng.AssertJUnit.assertEquals;

public class AerospikeBatchGroupAggregatorTest {
  private static final int BASE_TS = 1614556800;
  private static final int SECONDS_IN_SEGMENT = 3600 * 2;
  private static final int SECONDS_IN_RECORD = SECONDS_IN_SEGMENT * 3;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static AerospikeBatchSourceFactory SOURCE_FACTORY;
  private static AerospikeClientPlugin AS_CLIENT;
  private static MockTSDB TSDB;

  private LTSAerospike asClient;
  private AerospikeBatchQueryNode.QR queryResult;
  private MetaTimeSeriesQueryResult metaResult;
  private MetaTimeSeriesQueryResult.GroupResult groupResult;
  private AerospikeBatchQueryNode queryNode;
  private QueryPipelineContext context;
  private QueryContext queryContext;
  private AerospikeBatchGroupAggregator ts;

  @BeforeClass
  public static void beforeClass() {
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
    SOURCE_FACTORY = new AerospikeBatchSourceFactory();

    if (!TSDB.getConfig().hasProperty(AerospikeSourceFactory.SECONDS_IN_SEGMENT_KEY)) {
      TSDB.getConfig().register(AerospikeSourceFactory.SECONDS_IN_SEGMENT_KEY,
              SECONDS_IN_SEGMENT,
              false,
              "Seconds in a segment.");
    }
    if (!TSDB.getConfig().hasProperty(AerospikeBatchSourceFactory.AS_BATCH_LIMIT_KEY)) {
      TSDB.getConfig().register(AerospikeBatchSourceFactory.AS_BATCH_LIMIT_KEY, 16, false, "UT");
    }
    if (!TSDB.getConfig().hasProperty(AerospikeBatchSourceFactory.AS_JOBS_PER_QUERY)) {
      TSDB.getConfig().register(AerospikeBatchSourceFactory.AS_JOBS_PER_QUERY, 4, false, "UT");
    }
  }

  @BeforeMethod
  public void before() {
    ts = new AerospikeBatchGroupAggregator();
    queryResult = mock(AerospikeBatchQueryNode.QR.class);
    metaResult = mock(MetaTimeSeriesQueryResult.class);
    groupResult = mock(MetaTimeSeriesQueryResult.GroupResult.class);
    asClient = mock(LTSAerospike.class);
    context = mock(QueryPipelineContext.class);
    queryContext = mock(QueryContext.class);
    //when(queryResult.metaResult()).thenReturn(metaResult);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(queryContext);
    when(AS_CLIENT.asClient()).thenReturn(asClient);
    when(asClient.secondsInRecord()).thenReturn(SECONDS_IN_RECORD);
  }

  @Test
  public void ctorsOptimized() throws Exception {
    setup(BASE_TS, BASE_TS + 3600, "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    assertEquals(BASE_TS, ts.queryStartTime);
    assertEquals(BASE_TS + 3600, ts.queryEndTime);
    assertEquals(AuraMetricsNumericArrayIterator.Agg.SUM, ts.dsAgg);
    assertNull(ts.nonOptimizedAggregator);
    assertTrue(ts.combinedAggregator instanceof ArraySumFactory.ArraySum);
    assertFalse(ts.reportingAverage);
  }

  @Test
  public void ctorsNonOptimized() throws Exception {
    setup(BASE_TS, BASE_TS + 3600, "1m", "p95", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    assertEquals(BASE_TS, ts.queryStartTime);
    assertEquals(BASE_TS + 3600, ts.queryEndTime);
    assertEquals(AuraMetricsNumericArrayIterator.Agg.NON_OPTIMIZED, ts.dsAgg);
    assertEquals("p95", ts.nonOptimizedAggregator.name());
    assertNotNull(ts.nonOptimizedPooled);
    assertNotNull(ts.nonOptimizedDp);
    assertTrue(ts.combinedAggregator instanceof ArraySumFactory.ArraySum);
    assertFalse(ts.reportingAverage);
  }

  @Test
  public void ctorsReportingAverage() throws Exception {
    setup(BASE_TS, BASE_TS + 3600, "1m", "avg", "10s", false, "sum", false, null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    assertEquals("SUM", ts.dsAgg.toString());
    assertTrue(ts.reportingAverage);
  }

  @DataProvider
  public static Object[][] gbAligned1Segment2SeriesData() {
    return new Object[][] {
            {"sum", "sum", "1m", 60, 3d},
            {"sum", "avg", "1m", 60, 1.5d},
            {"sum", "max", "1m", 60, 2d},
            {"sum", "min", "1m", 60, 1d},
            {"sum", "count", "1m", 60, 2d},
            {"last", "sum", "1m", 60, 3d},
            {"avg", "sum", "1m", 60, 3d},
            {"count", "sum", "1m", 60, 2d},
            {"max", "sum", "1m", 60, 3d},
            {"sum", "last", "1m", 60, 2d}, // NOTE: not really useful for a GB.
            {"sum", "sum", "5m", 12, 15d},
            {"sum", "avg", "5m", 12, 7.5},
            {"sum", "max", "5m", 12, 10d},
            {"sum", "min", "5m", 12, 5d},
            {"avg", "sum", "5m", 12, 3d},
            {"min", "sum", "5m", 12, 3d},
            {"max", "sum", "5m", 12, 3d},
            {"sum", "count", "5m", 12, 2d},
            {"count", "sum", "5m", 12, 10d},
    };
  }

  @Test(dataProvider = "gbAligned1Segment2SeriesData")
  public void gbAligned1Segment2Series(String dsAgg,
                                       String gbAgg,
                                       String interval,
                                       Integer length,
                                       Double expectedValue) throws Exception {
    setup(BASE_TS, BASE_TS + 3600, interval, dsAgg, false, gbAgg, null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, false), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, false), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals((int) length, tsv.value().end());
    double[] expected = new double[length];
    Arrays.fill(expected, expectedValue);
    if (!gbAgg.equals("count")) {
      assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
    } else {
      long[] expectedLongs = new long[length];
      for (int i = 0; i < length; i++) {
        expectedLongs[i] = (long) expected[i];
      }
      assertArrayEquals(expectedLongs, tsv.value().longArray());
    }
  }

  @Test
  public void gbWithinSegment1Segment2Series() throws Exception {
    setup(BASE_TS + 300, BASE_TS + 900, "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(10, tsv.value().end());
    double[] expected = new double[10];
    double value = 13;
    for (int i = 0; i < expected.length; i++) {
      expected[i] = value;
      value += 2;
    }
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @Test
  public void gbCrossSegment() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(10, tsv.value().end());
    double[] expected = new double[10];
    double value = 233;
    for (int i = 0; i < expected.length; i++) {
      expected[i] = value;
      value += 2;
    }
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @Test
  public void gb3SegmentsMissingFirst() throws Exception {
    setup(BASE_TS, BASE_TS + (3600 * 6), "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(360, tsv.value().end());
    double[] expected = new double[360];
    Arrays.fill(expected, Double.NaN);
    double value = 243;
    for (int i = 120; i < expected.length; i++) {
      expected[i] = value;
      value += 2;
    }
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void gb3SegmentsMissingFirst2() throws Exception {
    setup(BASE_TS, BASE_TS + (3600 * 6), "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(360, tsv.value().end());
    double[] expected = new double[360];
    Arrays.fill(expected, Double.NaN);
    double value = 483;
    for (int i = 240; i < expected.length; i++) {
      expected[i] = value;
      value += 2;
    }
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void gb3SegmentsMissingMiddle() throws Exception {
    setup(BASE_TS, BASE_TS + (3600 * 6), "1m", "sum", false, "sum", null, "host");

    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true),24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(360, tsv.value().end());
    double[] expected = new double[360];
    Arrays.fill(expected, Double.NaN);
    double value = 3;
    for (int i = 0; i < 120; i++) {
      expected[i] = value;
      value += 2;
    }
    value = 483;
    for (int i = 240; i < expected.length; i++) {
      expected[i] = value;
      value += 2;
    }
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void gb3SegmentsMissingEnd() throws Exception {
    setup(BASE_TS, BASE_TS + (3600 * 6), "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(360, tsv.value().end());
    double[] expected = new double[360];
    Arrays.fill(expected, Double.NaN);
    double value = 3;
    for (int i = 0; i < 240; i++) {
      expected[i] = value;
      value += 2;
    }
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void gb3SegmentsMissingBothEnd() throws Exception {
    setup(BASE_TS, BASE_TS + (3600 * 6), "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(360, tsv.value().end());
    double[] expected = new double[360];
    Arrays.fill(expected, Double.NaN);
    double value = 3;
    for (int i = 0; i < 120; i++) {
      expected[i] = value;
      value += 2;
    }
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void gbNoData() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
  }

  @Test
  public void gbSkipEarlySegment() throws Exception {
    setup(BASE_TS + (3600 * 2) + 300, BASE_TS + (3600 * 2) + 900, "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(10, tsv.value().end());
    double[] expected = new double[10];
    double value = 253;
    for (int i = 0; i < expected.length; i++) {
      expected[i] = value;
      value += 2;
    }
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @Test
  public void gbDataBeforeQueryTime() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, 5, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, 5, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
  }

  @Test
  public void gbDataAfterQueryTime() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 1, 5, false), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 1, 5, false), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
  }

  @Test
  public void gbOutOfOrderSegment() throws Exception {
    setup(BASE_TS, BASE_TS + (3600 * 6), "1m", "sum", false, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    verify(queryNode, times(1)).onError(any(QueryDownstreamException.class));
  }

  @DataProvider
  public static Object[][] rateBasicData() {
    double[] oneSecond = new double[120];
    Arrays.fill(oneSecond, 0.0333333);
    oneSecond[0] = Double.NaN;

    double[] oneMinute = new double[120];
    Arrays.fill(oneMinute, 2);
    oneMinute[0] = Double.NaN;
    return new Object[][] {
            {"1s", oneSecond},
            {"1m", oneMinute},
    };
  }

  @Test(dataProvider = "rateBasicData")
  public void rateBasic(String interval, double[] expected) throws Exception {
    RateConfig rate = RateConfig.newBuilder()
            .setInterval(interval)
            .setId("rate")
            .addSource("m1")
            .build();
    setup(BASE_TS, BASE_TS + 3600 * 2, "1m", "sum", false, "sum", rate, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 120, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 120, true),24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(120, tsv.value().end());
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void rateBasicMissingMiddleInterval() throws Exception {
    RateConfig rate = RateConfig.newBuilder()
            .setInterval("1s")
            .setId("rate")
            .addSource("m1")
            .build();
    setup(BASE_TS, BASE_TS + (3600 * 6), "1m", "sum", false, "sum", rate, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true),24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(360, tsv.value().end());
    double[] expected = new double[360];
    Arrays.fill(expected, Double.NaN);
    double value = 0.033333;
    for (int i = 1; i < 120; i++) {
      expected[i] = value;
    }
    for (int i = 240; i < expected.length; i++) {
      expected[i] = value;
    }
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void rateDeltaOnly() throws Exception {
    RateConfig rate = RateConfig.newBuilder()
            .setInterval("1s")
            .setDeltaOnly(true)
            .setId("rate")
            .addSource("m1")
            .build();
    setup(BASE_TS, BASE_TS + 3600 * 2, "1m", "sum", false, "sum", rate, "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, true), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, true), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(120, tsv.value().end());
    double[] expected = new double[120];
    Arrays.fill(expected, 2);
    expected[0] = Double.NaN;
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @DataProvider
  public static Object[][] rateCountersData() {
    double[] notCounter = new double[120];
    Arrays.fill(notCounter, 0.033333);
    notCounter[0] = Double.NaN;
    notCounter[10] = 136.18333333; // the goofy value
    notCounter[11] = -136.116666;

    double[] isCounterNoReset = new double[120];
    Arrays.fill(isCounterNoReset, 0.033333);
    isCounterNoReset[0] = Double.NaN;
    isCounterNoReset[10] = 136.18333333; // the goofy value
    isCounterNoReset[11] = 3.0744573456182573E17;

    double[] reset = new double[120];
    Arrays.fill(reset, 0.033333);
    reset[0] = Double.NaN;
    reset[10] = 136.18333333; // the goofy value
    reset[11] = Double.NaN;

    double[] counterMax = new double[120];
    Arrays.fill(counterMax, 0.033333);
    counterMax[0] = Double.NaN;
    counterMax[10] = 136.18333333; // the goofy value
    counterMax[11] = 136.95;

    return new Object[][] {
            {false, false, 0, Long.MAX_VALUE, notCounter},
            {true, false, 0, Long.MAX_VALUE, isCounterNoReset},
            {true, true, 0, Long.MAX_VALUE, reset},
            {true, false, Long.MAX_VALUE - 2, Long.MAX_VALUE, isCounterNoReset},
            {true, false, 4096, Long.MAX_VALUE, reset},
            {true, false, 0, 8192, counterMax}
    };
  }

  @Test(dataProvider = "rateCountersData")
  public void rateCounter(boolean isCounter, boolean reset, long resetValue, long maxCounter, double[] expected) throws Exception {
    RateConfig rate = RateConfig.newBuilder()
            .setInterval("1s")
            .setId("rate")
            .setCounter(isCounter)
            .setDropResets(reset)
            .setResetValue(resetValue)
            .setCounterMax(maxCounter)
            .addSource("m1")
            .build();
    setup(BASE_TS, BASE_TS + 3600 * 2, "1m", "sum", false, "sum", rate, "host");
    ts.reset(0, 1, queryResult, metaResult);
    MockTimeSeriesEncoder enc = new MockTimeSeriesEncoder(BASE_TS, 1, true);
    enc.rateCounter = true;
    ts.addSegment(enc, 42);
    enc = new MockTimeSeriesEncoder(BASE_TS, 2, true);
    enc.rateCounter = true;
    ts.addSegment(enc, 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(120, tsv.value().end());
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @DataProvider
  public static Object[][] rateToCountData() {
    double[] defaultReporting = new double[120];
    Arrays.fill(defaultReporting, 180);
    defaultReporting[0] = Double.NaN;

    double[] defaultReporting1m = new double[120];
    Arrays.fill(defaultReporting1m, 3);
    defaultReporting1m[0] = Double.NaN;

    double[] defaultReporting5m = new double[120];
    Arrays.fill(defaultReporting5m, 0.6);
    defaultReporting5m[0] = Double.NaN;

    double[] reporting1s10s = new double[120];
    Arrays.fill(reporting1s10s, 30);
    reporting1s10s[0] = Double.NaN;

    double[] reporting1s1m = new double[120];
    Arrays.fill(reporting1s1m, 180);
    reporting1s1m[0] = Double.NaN;

    double[] reporting1m10s = new double[120];
    Arrays.fill(reporting1m10s, 3);
    reporting1m10s[0] = Double.NaN;

    return new Object[][] {
            {"1s", null, defaultReporting},
            {"1m", null, defaultReporting1m},
            {"5m", null, defaultReporting5m},
            {"1s", "10s", reporting1s10s},
            {"1s", "1m", reporting1s1m},
            {"1s", "5m", reporting1s1m}, // note we fall back to real data
            {"1m", "10s", reporting1m10s},
    };
  }

  @Test(dataProvider = "rateToCountData")
  public void rateToCount(String interval, String dataInterval, double[] expected) throws Exception {
    RateConfig.Builder builder = RateConfig.newBuilder()
            .setInterval(interval)
            .setRateToCount(true)
            .setId("rate")
            .addSource("m1");
    if (dataInterval != null) {
      builder.setDataInterval(dataInterval);
    }
    setup(BASE_TS, BASE_TS + 3600 * 2, "1m", "sum", false, "sum", builder.build(), "host");
    ts.reset(0, 1, queryResult, metaResult);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 1, false), 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, false), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(120, tsv.value().end());
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void rateToCountMissingLargeReportingInterval() throws Exception {
    RateConfig rate = RateConfig.newBuilder()
            .setInterval("1s")
            .setRateToCount(true)
            .setDataInterval("5m")
            .setId("rate")
            .addSource("m1")
            .build();
    setup(BASE_TS, BASE_TS + 3600 * 2, "1m", "sum", false, "sum", rate, "host");
    ts.reset(0, 1, queryResult, metaResult);
    MockTimeSeriesEncoder enc = new MockTimeSeriesEncoder(BASE_TS, 1, true);
    enc.rateMissing = true;
    ts.addSegment(enc, 42);
    enc = new MockTimeSeriesEncoder(BASE_TS, 2, true);
    enc.rateMissing = true;
    ts.addSegment(enc, 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(120, tsv.value().end());
    double[] expected = new double[120];
    Arrays.fill(expected, 180);
    expected[0] = Double.NaN;
    for (int i = 10; i < 25; i++) {
      expected[i] = Double.NaN;
    }
    expected[25] = 900;
    assertWithNans(expected, tsv.value().doubleArray());
  }

  // TODO - since infectious nan would blow out all values with the way we're resetting
  // an array, we can't allow it in downsampling.
  @Test
  public void dsInfectiousNan() throws Exception {
    setup(BASE_TS, BASE_TS + 3600 * 2, "5m", "avg", true, "sum", null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    MockTimeSeriesEncoder enc = new MockTimeSeriesEncoder(BASE_TS, 1, true);
    enc.rateMissing = true;
    ts.addSegment(enc, 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, false), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(24, tsv.value().end());
    double[] expected = new double[24];
    Arrays.fill(expected, 3);
    expected[2] = 2;
    expected[3] = 2;
    expected[4] = 2;
    assertWithNans(expected, tsv.value().doubleArray());
  }

  // TODO - Need to implement this as well.
  @Test
  public void gbInfectiousNaN() throws Exception {
    setup(BASE_TS, BASE_TS + 3600 * 2, "5m", "avg", null, false, "sum", true, null, "host");
    ts.reset(0, 1, queryResult, metaResult);
    MockTimeSeriesEncoder enc = new MockTimeSeriesEncoder(BASE_TS, 1, true);
    enc.rateMissing = true;
    ts.addSegment(enc, 42);
    ts.addSegment(new MockTimeSeriesEncoder(BASE_TS, 2, false), 24);
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, ts.flush());

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) finished.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(24, tsv.value().end());
    double[] expected = new double[24];
    Arrays.fill(expected, 3);
    expected[2] = 2;
    expected[3] = 2;
    expected[4] = 2;
//    expected[2] = Double.NaN;
//    expected[3] = Double.NaN;
//    expected[4] = Double.NaN;
    assertWithNans(expected, tsv.value().doubleArray());
  }

  @Test
  public void scheduling() {
    Random rnd = new Random(System.currentTimeMillis());
    int threads = 4;
    int batchLimit = 16;
    int minPerJob = 16;

    class MR {
      int ng = rnd.nextInt(1204);
      int[] groups = new int[ng];
      int total;

      MR() {
        for (int i = 0; i < ng; i++) {
          groups[i] = rnd.nextInt(4096);
          if (groups[i] == 0) {
            groups[i] = 1;
          }
        }
//        groups[0] = 1;
//        groups[1] = 1;
//        groups[2] = 2;
//        groups[3] = 25;
//        groups[4] = 3;
//        groups[5] = 1;
//        groups[6] = 93;
//        groups[7] = 14;
//        groups[8] = 6;
//        groups[9] = 5;
//        groups[10] = 8;
//        groups[11] = 11;


        for (int i = 0; i < ng; i++) {
          total += groups[i];
        }
      }

      int total() {
        return total;
      }

      int numGroups() {
        return ng;
      }

      int grp(int idx) {
        return groups[idx];
      }

    }

    final MR mr = new MR();

    class Job {
      int gbIdx;
      int endGbIndex;
      int hashIdx;
      int endHashIdx;

      @Override
      public String toString() {
        return "GBI: " + gbIdx + " EGBI: " + endGbIndex + " HI: " + hashIdx + " EHI: " + endHashIdx;
      }

      int runThrough() {
        int wouldRead = 0;
        while (true) {
          int numHashes = mr.groups[gbIdx];
          // we do the time range and batches here.
          if (hashIdx > 0) {
            if (endGbIndex == gbIdx && endHashIdx > 0) {
              // range within this group and that'd be it!
              int delta = endHashIdx - hashIdx;
              assertTrue(delta < numHashes);
              wouldRead += delta;
              return wouldRead;
            } else {
              // in this case, we ignore the end hash. If set, it's set
              // for the final GB index we're reading.
              wouldRead += numHashes - hashIdx;
              hashIdx = 0;
              gbIdx++;
            }
          } else if (endHashIdx > 0) {
            if (endGbIndex == gbIdx) {
              // end
              int delta = endHashIdx - hashIdx;
              wouldRead += delta;
              return wouldRead;
            } else {
              // read all through this group
              wouldRead += numHashes;
              gbIdx++;
            }
          } else {
            // read all through this group
            wouldRead += numHashes;
            gbIdx++;
          }

          if (gbIdx >= endGbIndex && endHashIdx == 0) {
            break;
          }
        }
        return wouldRead;
      }
    }


    List<Job> jobs = Lists.newArrayList();

    System.out.println(mr.total);
    int hashesPerJob = Math.max(minPerJob,
            (int) Math.ceil((double) mr.total / (double) threads));
    System.out.println("Total series per job: " + hashesPerJob);
    int curHashes = 0;
    Job job = new Job();

    for (int i = 0; i < mr.numGroups(); i++) {
      int numHashes = mr.groups[i];
      //System.out.println("Working group " + i + " with " + numHashes + " hashes.  CH: " + curHashes);
      if (curHashes + numHashes >= hashesPerJob) {
        // new job
        int delta = hashesPerJob - curHashes;
        job.endGbIndex = i;

        int newJobhHashIdx;
        if (job.endGbIndex == job.gbIdx) {
          newJobhHashIdx = job.hashIdx + delta;
        } else {
          newJobhHashIdx = delta;
        }
        job.endHashIdx = newJobhHashIdx;
        jobs.add(job);
        //System.out.println("Wrap job. CH: " + curHashes + " Delta: " + delta + " {" + job + "}");

        int remaining = numHashes - delta;
        while (remaining >= hashesPerJob) {
          job = new Job();
          job.gbIdx = i;
          job.hashIdx = newJobhHashIdx;
          job.endGbIndex = i;
          job.endHashIdx = job.hashIdx + hashesPerJob;
          remaining -= hashesPerJob;
          newJobhHashIdx += hashesPerJob;
          jobs.add(job);
        }

        curHashes = remaining;
        //System.out.println("        REMAINING: " + remaining);
        job = new Job();
        if (remaining == 0 || newJobhHashIdx >= numHashes) {
          job.gbIdx = i + 1;
          job.hashIdx = 0;
        } else {
          job.gbIdx = i;
          job.hashIdx = newJobhHashIdx;
        }
      } else {
        curHashes += numHashes;
      }
    }

    if (curHashes > 0) {
      //System.out.println("Remainder: " + curHashes);
      job.endGbIndex = mr.numGroups() - 1;
      job.endHashIdx = mr.groups[mr.numGroups() - 1];
      jobs.add(job);
    }

    int wouldRead = 0;
    //System.out.println(jobs.get(2));
    //System.out.println("Would read: " + jobs.get(2).runThrough());
    for (int i = 0; i < jobs.size(); i++) {
      System.out.println(jobs.get(i));
      int wr = jobs.get(i).runThrough();
      wouldRead += wr;
      System.out.println("  WOULD Read: " + wr);
    }
    System.out.println("Read " + wouldRead + " out of " + mr.total);

    if (wouldRead != mr.total) {
      for (int i = 0; i < mr.numGroups(); i++) {
        System.out.println("groups[" + i + "] = " + mr.groups[i] + ";");
      }
      assertFalse(true);
    }
  }

  // ----------------------- UTILITIES ------------------------------

  void setup(int start, int end, String interval, String dsAgg, boolean dsInfectiousNan, String gbAgg, RateConfig rateConfig, String... tagKeys) throws Exception {
    setup(start, end, interval, dsAgg, null, dsInfectiousNan, gbAgg, false, rateConfig, tagKeys);
  }

  void setup(int start,
             int end,
             String interval,
             String dsAgg,
             String reportingInterval,
             boolean dsInfectiousNan,
             String gbAgg,
             boolean gbInfectiousNan,
             RateConfig rateConfig,
             String... tagKeys) throws Exception {
    DownsampleConfig.Builder builder = DownsampleConfig.newBuilder()
            .setAggregator(dsAgg)
            .setInterval(interval)
            .setInfectiousNan(dsInfectiousNan)
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setStart(Integer.toString(start))
            .setEnd(Integer.toString(end))
            .addSource(rateConfig == null ? "m1" : rateConfig.getId())
            .setId("ds");
    if (reportingInterval != null) {
      builder.setReportingInterval(reportingInterval);
    }
    DownsampleConfig ds = builder.build();

    GroupByConfig gb =
            GroupByConfig.newBuilder()
                    .setAggregator(gbAgg)
                    .setTagKeys(Sets.newHashSet(tagKeys))
                    .setInfectiousNan(gbInfectiousNan)
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .setId("gb")
                    .addSource("ds")
                    .build();

    List<QueryNodeConfig> pushdowns = Lists.newArrayList();
    if (rateConfig != null) {
      pushdowns.add(rateConfig);
    }
    pushdowns.add(ds);
    pushdowns.add(gb);
    DefaultTimeSeriesDataSourceConfig config = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.usr")
                    .build())
            .setPushDownNodes(pushdowns)
            .setId("m1")
            .build();
    queryNode = Mockito.spy(new AerospikeBatchQueryNode(SOURCE_FACTORY,
            context, config, false));

    SemanticQuery query =
            SemanticQuery.newBuilder()
                    .setMode(QueryMode.SINGLE)
                    .addExecutionGraphNode(config)
                    .setStart(Integer.toString(start))
                    .setEnd(Integer.toString(end))
                    .build();
    when(context.query()).thenReturn(query);
    queryNode.initialize(null).join();

    when(queryResult.source()).thenReturn(queryNode);
  }

  class MockTimeSeriesEncoder implements RawTimeSeriesEncoder {
    int segmentTime;
    double value;
    int frequency;
    boolean increment;
    int count;
    boolean start;
    boolean rateCounter;
    boolean rateMissing;

    MockTimeSeriesEncoder(int segmentTime, double value, boolean increment) {
      this.segmentTime = segmentTime;
      this.value = value;
      this.increment = increment;
      frequency = 60;
    }

    MockTimeSeriesEncoder(int segmentTime, double value, int count, boolean start) {
      this.segmentTime = segmentTime;
      this.value = value;
      this.count = count;
      this.start = start;
      increment = false;
      frequency = 60;
    }

    @Override
    public void collectMetrics() {

    }

    @Override
    public void setTags(String[] tags) {

    }

    @Override
    public long createSegment(int segmentTime) {
      return 0;
    }

    @Override
    public void openSegment(long segmentAddress) {

    }

    @Override
    public void addDataPoint(int timestamp, double value) {

    }

    @Override
    public void read(TSDataConsumer consumer) {

    }

    @Override
    public int readAndDedupe(double[] valueBuffer) {
      if (rateMissing) {
        double val = value;
        int wrote = 0;
        for (int i = 0; i < valueBuffer.length; i++) {
          if (i % 60 == 0 ) {
            if (i / 60 >= 10 && i / 60 < 25) {
              continue;
            }
            valueBuffer[i] = val;
            ++wrote;
          }
        }
        return wrote;
      }

      if (rateCounter) {
        int wrote = 0;
        double val = value;
        for (int i = 0; i < valueBuffer.length; i++) {
          if (i % 60 == 0) {
            valueBuffer[i] = val++;
            ++wrote;

            if (i / 60 == 10) {
              valueBuffer[i] = 4096;
            }
          }
        }
        return wrote;
      }

      for (int i = 0; i < valueBuffer.length; i++) {
        if (count != 0) {
          if (start && i / frequency < count) {
            if (i % frequency == 0) {
              valueBuffer[i] = value;
              if (increment) {
                value++;
              }
            }
          } else if (!start && (i / frequency) >= (valueBuffer.length - count)) {
            if (i % frequency == 0) {
              valueBuffer[i] = value;
              if (increment) {
                value++;
              }
            }
          }
        } else {
          if (i % frequency == 0) {
            valueBuffer[i] = value;
            if (increment) {
              value++;
            }
          }
        }
      }
      return SECONDS_IN_SEGMENT / frequency;
    }

    @Override
    public int getSegmentTime() {
      return segmentTime;
    }

    @Override
    public int getNumDataPoints() {
      return SECONDS_IN_SEGMENT / frequency;
    }

    @Override
    public void freeSegment() {

    }

    @Override
    public void collectSegment(long segmentAddress) {

    }

    @Override
    public void freeCollectedSegments() {

    }

    @Override
    public boolean segmentIsDirty() {
      return false;
    }

    @Override
    public boolean segmentHasOutOfOrderOrDuplicates() {
      return false;
    }

    @Override
    public void markSegmentFlushed() {

    }

    @Override
    public int serializationLength() {
      return 0;
    }

    @Override
    public void serialize(byte[] buffer, int offset, int length) {

    }
  }

  void assertWithNans(double[] expected, double[] test) {
    if (expected.length != test.length) {
      fail("Arrays have different lengths. Expected [" + expected.length + "] but got [" + test.length + "]");
    }
    for (int i = 0; i < expected.length; i++) {
      if (Double.isNaN(expected[i])) {
        if (!Double.isNaN(test[i])) {
          fail("Expected [NaN] at " + i + " but got [" + test[i] + "]");
        }
        continue;
      }
      try {
        assertEquals(expected[i], test[i], 0.0001);
      } catch (AssertionError e) {
        fail("Expected [" + expected[i] + "] at " + i + " but got [" + test[i] + "]");
      }
    }
  }

}