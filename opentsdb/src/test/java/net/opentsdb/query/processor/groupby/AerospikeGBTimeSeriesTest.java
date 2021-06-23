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

package net.opentsdb.query.processor.groupby;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.core.LongTermStorage;
import net.opentsdb.aura.metrics.core.TSDataConsumer;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoder;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.storage.AerospikeClientPlugin;
import net.opentsdb.aura.metrics.storage.AerospikeGBQueryResult;
import net.opentsdb.aura.metrics.storage.AerospikeQueryNode;
import net.opentsdb.aura.metrics.storage.AerospikeSourceFactory;
import net.opentsdb.aura.metrics.storage.AuraMetricsNumericArrayIterator;
import net.opentsdb.common.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesStringId;
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
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.utils.UnitTestException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

/**
 * NOTE TESTING:
 * - OOO within a segment as that's handle by the decoder.
 * - OOO segments as that should never happen currently.
 *
 * TODO
 * - DS only
 * - Rate only
 * - Rate and DS only
 * - DS then rate ONLY
 * - DS, GB, rate
 *
 * - Rate with segments and all permutations
 *
 */
public class AerospikeGBTimeSeriesTest {
  private static final int BASE_TS = 1614556800;
  private static final int SECONDS_IN_SEGMENT = 3600 * 2;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static AerospikeSourceFactory SOURCE_FACTORY;
  private static AerospikeClientPlugin AS_CLIENT;
  private static MockTSDB TSDB;
  private static TimeSeriesEncoder THROW_ON_NEXT;
  private static TimeSeriesEncoder THROW_ON_SEGMENT_TIME;
  private static TimeSeriesEncoder THROW_ON_DECODE;

  private LTSAerospike asClient;
  private AerospikeGBQueryResult queryResult;
  private MetaTimeSeriesQueryResult metaResult;
  private MetaTimeSeriesQueryResult.GroupResult groupResult;
  private AerospikeQueryNode queryNode;
  private QueryPipelineContext context;
  private QueryContext queryContext;

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
    SOURCE_FACTORY = new AerospikeSourceFactory();
    THROW_ON_NEXT = mock(TimeSeriesEncoder.class);
    THROW_ON_SEGMENT_TIME = mock(TimeSeriesEncoder.class);
    when(THROW_ON_SEGMENT_TIME.getSegmentTime()).thenThrow(new UnitTestException());
    THROW_ON_DECODE = mock(TimeSeriesEncoder.class);
    when(THROW_ON_DECODE.readAndDedupe(any(double[].class))).thenThrow(new UnitTestException());
    when(THROW_ON_DECODE.getSegmentTime()).thenReturn(BASE_TS);
    if (!TSDB.getConfig().hasProperty(AerospikeSourceFactory.SECONDS_IN_SEGMENT_KEY)) {
      TSDB.getConfig().register(AerospikeSourceFactory.SECONDS_IN_SEGMENT_KEY,
              SECONDS_IN_SEGMENT,
              false,
              "Seconds in a segment.");
    }
  }

  @BeforeMethod
  public void before() {
    queryResult = mock(AerospikeGBQueryResult.class);
    metaResult = mock(MetaTimeSeriesQueryResult.class);
    groupResult = mock(MetaTimeSeriesQueryResult.GroupResult.class);
    asClient = mock(LTSAerospike.class);
    context = mock(QueryPipelineContext.class);
    queryContext = mock(QueryContext.class);
    when(queryResult.metaResult()).thenReturn(metaResult);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(queryContext);
    when(AS_CLIENT.asClient()).thenReturn(asClient);
  }

  @Test
  public void ctorsOptimized() throws Exception {
    setup(BASE_TS, BASE_TS + 3600, "1m", "sum", false, "sum", null, "host");
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
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
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
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
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals("SUM", ts.dsAgg.toString());
    assertTrue(ts.reportingAverage);
  }

  @Test
  public void timeSeriesId() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    when(queryResult.metricString()).thenReturn("mymetric");
    when(metaResult.getStringForHash(24L)).thenReturn("web01");
    when(metaResult.getStringForHash(42L)).thenReturn("den");
    when(groupResult.tagHashes()).thenReturn(new MetaTimeSeriesQueryResult.GroupResult.TagHashes() {
      int idx = 0;
      @Override
      public int size() {
        return 2;
      }

      @Override
      public long next() {
        return idx++ > 0 ? 42 : 24;
      }
    });
    when(queryNode.gbKeys()).thenReturn(new String[] { "host", "dc" });

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("mymetric", id.metric());
    Map<String, String> tags = id.tags();
    assertEquals(2, tags.size());
    assertEquals("web01", tags.get("host"));
    assertEquals("den", tags.get("dc"));
    verify(queryNode, never()).onError(any(QueryDownstreamException.class));

    // cached
    id = (TimeSeriesStringId) ts.id();
    assertEquals("mymetric", id.metric());
    verify(queryNode, times(1)).gbKeys();
    tags = id.tags();
    assertEquals(2, tags.size());
    assertEquals("web01", tags.get("host"));
    assertEquals("den", tags.get("dc"));

    // just to satisfy coverage.
    assertNull(id.alias());
    assertNull(id.namespace());
    assertEquals("web01", id.getTagValue("host"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    assertNull(id.uniqueIds());
    assertEquals(0, id.hits());
    assertEquals(0, id.compareTo(null));
    assertFalse(id.encoded());
    assertEquals(Const.TS_STRING_ID, id.type());
    try {
      id.buildHashCode();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }

  @Test
  public void timeSeriesIdGroupByMissMatch() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");when(queryResult.metricString()).thenReturn("mymetric");
    when(metaResult.getStringForHash(24L)).thenReturn("web01");
    when(metaResult.getStringForHash(42L)).thenReturn("den");
    when(groupResult.tagHashes()).thenReturn(new MetaTimeSeriesQueryResult.GroupResult.TagHashes() {
      int idx = 0;
      @Override
      public int size() {
        return 2;
      }

      @Override
      public long next() {
        return idx++ > 0 ? 42 : 24;
      }
    });
    when(queryNode.gbKeys()).thenReturn(new String[] { "host"/*, "dc" */});

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("mymetric", id.metric());
    Map<String, String> tags = id.tags();
    assertTrue(tags.isEmpty());
    verify(queryNode, times(1)).onError(any(QueryDownstreamException.class));
  }

  @Test
  public void timeSeriesIdGroupByException() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");when(queryResult.metricString()).thenReturn("mymetric");
    when(metaResult.getStringForHash(24L)).thenReturn("web01");
    when(metaResult.getStringForHash(42L)).thenReturn("den");
    when(groupResult.tagHashes()).thenReturn(new MetaTimeSeriesQueryResult.GroupResult.TagHashes() {
      int idx = 0;
      @Override
      public int size() {
        return 2;
      }

      @Override
      public long next() {
        if (idx++ > 0) {
          throw new RuntimeException("BOO");
        }
        return 24;
      }
    });
    when(queryNode.gbKeys()).thenReturn(new String[] { "host", "dc"});

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    assertEquals("mymetric", id.metric());
    Map<String, String> tags = id.tags();
    assertEquals(1, tags.size());
    verify(queryNode, times(1)).onError(any(QueryDownstreamException.class));
  }

  @Test
  public void gbIterator() throws Exception {
    // satisfy coverage
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    assertEquals(NumericArrayType.TYPE, it.getType());

    // since the iterator returns itself as a ref, this is fine.
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(BASE_TS + (3600 * 2) - 300, tsv.timestamp().epoch());
    assertEquals(NumericArrayType.TYPE, tsv.type());
  }

  @DataProvider
  public static Object[][] gbAligned1Segment2Series1JobData() {
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

  @Test(dataProvider = "gbAligned1Segment2Series1JobData")
  public void gbAligned1Segment2Series1Job(String dsAgg,
                                           String gbAgg,
                                           String interval,
                                           Integer length,
                                           Double expectedValue) throws Exception {
    setup(BASE_TS, BASE_TS + 3600, interval, dsAgg, false, gbAgg, null, "host");
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteConsistent(1), new OneMinuteConsistent(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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

  @DataProvider
  public static Object[][] gbAligned1Segment4Series2JobsData() {
    return new Object[][] {
            {"sum", "sum", "1m", 60, 10d},
            {"sum", "avg", "1m", 60, 2.5d},
            {"sum", "max", "1m", 60, 4d},
            {"sum", "min", "1m", 60, 1d},
            {"avg", "sum", "1m", 60, 10d},
            {"sum", "sum", "5m", 12, 50d},
            {"sum", "avg", "5m", 12, 12.5},
            {"sum", "max", "5m", 12, 20d},
            {"sum", "min", "5m", 12, 5d},
            {"avg", "sum", "5m", 12, 10d},
            {"min", "sum", "5m", 12, 10d},
    };
  }

  @Test(dataProvider = "gbAligned1Segment4Series2JobsData")
  public void gbAligned1Segment4Series2Jobs(String dsAgg,
                                            String gbAgg,
                                            String interval,
                                            Integer length,
                                            Double expectedValue) throws Exception {
    Answer<Integer>[] answers = new Answer[] {
            new OneMinuteConsistent(1),
            new OneMinuteConsistent(2),
            new OneMinuteConsistent(3),
            new OneMinuteConsistent(4)
    };
    setup(BASE_TS, BASE_TS + 3600, interval, dsAgg, false, gbAgg, null, "host");
    dataSetup(4, 42L, 4, answers);
    when(queryNode.seriesPerJob()).thenReturn(2);
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(2, ts.jobCount);
    assertEquals(2, ts.jobs.length);
    assertEquals(2, ts.aggrCount);
    assertEquals(2, ts.valuesCombiner.length);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals((int) length, tsv.value().end());
    double[] expected = new double[length];
    Arrays.fill(expected, expectedValue);
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @DataProvider
  public static Object[][] gbAligned1SegmentManySeriesManyJobsData() {
    return new Object[][] {
            {"sum", "sum", "1m", 60, 78d},
            {"sum", "avg", "1m", 60, 6.5d},
            {"sum", "max", "1m", 60, 12d},
            {"sum", "min", "1m", 60, 1d},
            {"avg", "sum", "1m", 60, 78d},
            {"sum", "sum", "5m", 12, 390d},
            {"sum", "avg", "5m", 12, 32.5},
            {"sum", "max", "5m", 12, 60d},
            {"sum", "min", "5m", 12, 5d},
            {"avg", "sum", "5m", 12, 78d},
            {"min", "sum", "5m", 12, 78d},
    };
  }

  @Test(dataProvider = "gbAligned1SegmentManySeriesManyJobsData")
  public void gbAligned1SegmentManySeriesManyJobs(String dsAgg,
                                                  String gbAgg,
                                                  String interval,
                                                  Integer length,
                                                  Double expectedValue) throws Exception {
    Answer<Integer>[] answers = new Answer[12];
    for (int i = 0; i < answers.length; i++) {
      answers[i] = new OneMinuteConsistent(i + 1);
    }
    setup(BASE_TS, BASE_TS + 3600, interval, dsAgg, false, gbAgg, null, "host");
    dataSetup(12, 42L, 12, answers);
    when(queryNode.getGbThreads()).thenReturn(2);
    when(queryNode.seriesPerJob()).thenReturn(2);
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(6, ts.jobCount);
    assertEquals(6, ts.jobs.length);
    assertEquals(2, ts.aggrCount);
    assertEquals(2, ts.valuesCombiner.length);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals((int) length, tsv.value().end());
    double[] expected = new double[length];
    Arrays.fill(expected, expectedValue);
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @Test
  public void gbWithinSegment1Segment2Series1Job() throws Exception {
    setup(BASE_TS + 300, BASE_TS + 900, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteIncrement(1),
            new OneMinuteIncrement(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
  public void gbWithinSegment1Segment2Series2Jobs() throws Exception {
    Answer<Integer>[] answers = new Answer[] {
            new OneMinuteIncrement(1),
            new OneMinuteIncrement(2),
            new OneMinuteIncrement(3),
            new OneMinuteIncrement(4) };
    setup(BASE_TS + 300, BASE_TS + 900, "1m", "sum", false, "sum", null, "host");
    dataSetup(4, 42L, 4, answers);
    when(queryNode.seriesPerJob()).thenReturn(2);
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(2, ts.jobCount);
    assertEquals(2, ts.jobs.length);
    assertEquals(2, ts.aggrCount);
    assertEquals(2, ts.valuesCombiner.length);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(10, tsv.value().end());
    double[] expected = new double[10];
    double value = 30;
    for (int i = 0; i < expected.length; i++) {
      expected[i] = value;
      value += 4;
    }
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @Test
  public void gbWithinSegment1SegmentManySeriesManyJobs() throws Exception {
    Answer<Integer>[] answers = new Answer[12];
    for (int i = 0; i < answers.length; i++) {
      answers[i] = new OneMinuteIncrement(i + 1);
    }
    setup(BASE_TS + 300, BASE_TS + 900, "1m", "sum", false, "sum", null, "host");
    dataSetup(12, 42L, 12, answers);
    when(queryNode.getGbThreads()).thenReturn(2);
    when(queryNode.seriesPerJob()).thenReturn(2);
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(6, ts.jobCount);
    assertEquals(6, ts.jobs.length);
    assertEquals(2, ts.aggrCount);
    assertEquals(2, ts.valuesCombiner.length);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(10, tsv.value().end());
    double[] expected = new double[10];
    double value = 138;
    for (int i = 0; i < expected.length; i++) {
      expected[i] = value;
      value += 12;
    }
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @Test
  public void gb1JobClosedEarly() throws Exception {
    when(queryContext.isClosed())
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);
    setup(BASE_TS + 300, BASE_TS + 900, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteIncrement(1),
            new OneMinuteIncrement(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(10, tsv.value().end());
    double[] expected = new double[10];
    double value = 6;
    for (int i = 0; i < expected.length; i++) {
      expected[i] = value++;
    }
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @Test
  public void gbManyJobsCloseEarly() throws Exception {
    when(queryContext.isClosed())
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(false)
            .thenReturn(true);
    Answer<Integer>[] answers = new Answer[12];
    for (int i = 0; i < answers.length; i++) {
      answers[i] = new OneMinuteIncrement(i + 1);
    }
    setup(BASE_TS + 300, BASE_TS + 900, "1m", "sum", false, "sum", null, "host");
    dataSetup(12, 42L, 12, answers);
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
  }

  @Test
  public void gbCrossSegment() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
  public void gbNoDataReadBothSeries() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(),
            new MockRecords());

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
  }

  @Test
  public void gbNoData1of2Series() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true)),
            new MockRecords());

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(10, tsv.value().end());
    double[] expected = new double[10];
    double value = 116;
    for (int i = 0; i < expected.length; i++) {
      expected[i] = value;
      value++;
    }
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
  }

  @Test
  public void gbDataBeforeQueryTime() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, 5, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, 5, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
  }

  @Test
  public void gbDataAfterQueryTime() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 1, 5, false)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 1, 5, false)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
  }

  @Test
  public void gbASExceptionFirstRead() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(THROW_ON_NEXT,
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    verify(queryNode, times(1)).onError(any(UnitTestException.class));
  }

  @Test
  public void gbASExceptionSecondRead() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    THROW_ON_NEXT),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    verify(queryNode, times(1)).onError(any(UnitTestException.class));
  }

  @Test
  public void gbASExceptionFirstDecode() throws Exception {
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(THROW_ON_DECODE,
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    verify(queryNode, times(1)).onError(any(UnitTestException.class));
  }

  @Test
  public void gbASExceptionSecondDecode() throws Exception {
    TimeSeriesEncoder tosser = mock(TimeSeriesEncoder.class);
    when(tosser.getSegmentTime()).thenReturn(BASE_TS + (3600 * 2));
    when(tosser.readAndDedupe(any(double[].class))).thenThrow(new UnitTestException());
    setup(BASE_TS + (3600 * 2) - 300, BASE_TS + (3600 * 2) + 300, "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    tosser),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    verify(queryNode, times(1)).onError(any(UnitTestException.class));
  }

  // Shouldn't happen but...
  @Test
  public void gbOutOfOrderSegment() throws Exception {
    setup(BASE_TS, BASE_TS + (3600 * 6), "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    /** OOO */      new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    assertEquals(1, ts.jobCount);
    assertNull(ts.jobs);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    verify(queryNode, times(1)).onError(any(QueryDownstreamException.class));
  }

  // Shouldn't happen but...
  @Test
  public void gbSegmentBeforeQueryRange() throws Exception {
    setup(BASE_TS + (3600 * 2), BASE_TS + (3600 * 4), "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertFalse(it.hasNext());
    verify(queryNode, times(1)).onError(any(QueryDownstreamException.class));
  }

  // Shouldn't happen but...
  @Test
  public void gbSegmentAfterQueryRange() throws Exception {
    setup(BASE_TS + (3600 * 2), BASE_TS + (3600 * 4), "1m", "sum", false, "sum", null, "host");
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 121, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS + (3600 * 2), 122, true)));

    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericArrayType> tsv = it.next();
    assertEquals(0, tsv.value().offset());
    assertEquals(120, tsv.value().end());
    double[] expected = new double[120];
    double value = 243;
    for (int i = 0; i < expected.length; i++) {
      expected[i] = value;
      value += 2;
    }
    assertArrayEquals(expected, tsv.value().doubleArray(), 0.001);
    verify(queryNode, never()).onError(any(QueryDownstreamException.class));
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
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteIncrement(1),
            new OneMinuteIncrement(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L,
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 1, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 241, true)),
            new MockRecords(new MockTimeSeriesEncoder(BASE_TS, 2, true),
                    new MockTimeSeriesEncoder(BASE_TS + (3600 * 4), 242, true)));
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteIncrement(1),
            new OneMinuteIncrement(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteRateCounter(1),
            new OneMinuteRateCounter(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteConsistent(1),
            new OneMinuteConsistent(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteRateDataIntervalMissing(1),
            new OneMinuteRateDataIntervalMissing(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteRateDataIntervalMissing(1),
            new OneMinuteConsistent(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    dataSetup(2, 42L, 2, new Answer[] {
            new OneMinuteRateDataIntervalMissing(1),
            new OneMinuteConsistent(2) });
    AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(queryResult, groupResult);
    ts.process();

    TypedTimeSeriesIterator<NumericArrayType> it =
            (TypedTimeSeriesIterator<NumericArrayType>) ts.iterator(NumericArrayType.TYPE).get();
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
    queryNode = spy(new AerospikeQueryNode(SOURCE_FACTORY,
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

  void dataSetup(int totalSeries, long groupId, int groupSize, Answer<Integer>... answers) {
    when(metaResult.totalHashes()).thenReturn(totalSeries);
    when(groupResult.id()).thenReturn(groupId);
    when(groupResult.numHashes()).thenReturn(groupSize);
    for (int i = 0; i < groupSize; i++) {
      when(groupResult.getHash(i)).thenReturn((long) i);
      LongTermStorage.Records records = mock(LongTermStorage.Records.class);
      int segmentsStart = queryNode.getSegmentsStart();
      int segmentsEnd = queryNode.getSegmentsEnd();
      when(asClient.read(eq((long) i), eq(segmentsStart), eq(segmentsEnd)))
              .thenReturn(records);
      when(records.hasNext())
              .thenReturn(true)
              .thenReturn(false);
      TimeSeriesEncoder enc1 = mockEncoder(BASE_TS, answers[i]);
      when(records.next()).thenReturn(enc1);
    }
  }

  void dataSetup(int totalSeries, long groupId, MockRecords... records) {
    when(metaResult.totalHashes()).thenReturn(totalSeries);
    when(groupResult.id()).thenReturn(groupId);
    when(groupResult.numHashes()).thenReturn(records.length);
    for (int i = 0; i < records.length; i++) {
      when(groupResult.getHash(i)).thenReturn((long) i);
      int segmentsStart = queryNode.getSegmentsStart();
      int segmentsEnd = queryNode.getSegmentsEnd();
      when(asClient.read(eq((long) i), eq(segmentsStart), eq(segmentsEnd)))
              .thenReturn(records[i]);
    }
  }

  TimeSeriesEncoder mockEncoder(int segmentTime, Answer<Integer> answer) {
    TimeSeriesEncoder encoder = mock(TimeSeriesEncoder.class);
    when(encoder.getSegmentTime()).thenReturn(segmentTime);
    when(encoder.readAndDedupe(any(double[].class))).thenAnswer(answer);
    return encoder;
  }

  class MockRecords implements LongTermStorage.Records {

    TimeSeriesEncoder[] encoders;
    int idx;

    MockRecords(TimeSeriesEncoder... encoders) {
      this.encoders = encoders;
    }

    @Override
    public boolean hasNext() {
      return idx < encoders.length;
    }

    @Override
    public TimeSeriesEncoder next() {
      TimeSeriesEncoder enc = encoders[idx++];
      if (enc == THROW_ON_NEXT) {
        throw new UnitTestException();
      }
      return enc;
    }
  }

  class MockTimeSeriesEncoder implements TimeSeriesEncoder {
    int segmentTime;
    double value;
    int frequency;
    boolean increment;
    int count;
    boolean start;

    MockTimeSeriesEncoder(int segmentTime, double value, boolean increment) {
      this.segmentTime = segmentTime;
      this.value = value;
      this.increment = increment;
      frequency = 60;
    }

    MockTimeSeriesEncoder(int segmentTime, double value, boolean increment, int frequency) {
      this.segmentTime = segmentTime;
      this.value = value;
      this.increment = increment;
      this.frequency = frequency;
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

  class OneMinuteConsistent implements Answer<Integer> {
    final int val;
    OneMinuteConsistent(final int val) {
      this.val = val;
    }

    @Override
    public Integer answer(InvocationOnMock invocation) throws Throwable {
      double[] values = (double[]) invocation.getArguments()[0];
      int wrote = 0;
      for (int i = 0; i < values.length; i++) {
        if (i % 60 == 0) {
          values[i] = val;
          ++wrote;
        }
      }
      return wrote;
    }
  };

  class OneMinuteIncrement implements Answer<Integer> {
    int val;
    OneMinuteIncrement(final int val) {
      this.val = val;
    }

    @Override
    public Integer answer(InvocationOnMock invocation) throws Throwable {
      double[] values = (double[]) invocation.getArguments()[0];
      int wrote = 0;
      for (int i = 0; i < values.length; i++) {
        if (i % 60 == 0) {
          values[i] = val++;
          ++wrote;
        }
      }
      return wrote;
    }
  };

  class OneMinuteRateCounter implements Answer<Integer> {
    int val;
    OneMinuteRateCounter(final int val) {
      this.val = val;
    }

    @Override
    public Integer answer(InvocationOnMock invocation) throws Throwable {
      double[] values = (double[]) invocation.getArguments()[0];
      int wrote = 0;
      for (int i = 0; i < values.length; i++) {
        if (i % 60 == 0) {
          values[i] = val++;
          ++wrote;

          if (i / 60 == 10) {
            values[i] = 4096;
          }
        }
      }
      return wrote;
    }
  }

  class OneMinuteRateDataIntervalMissing implements Answer<Integer> {
    int val;
    OneMinuteRateDataIntervalMissing(final int val) {
      this.val = val;
    }

    @Override
    public Integer answer(InvocationOnMock invocation) throws Throwable {
      double[] values = (double[]) invocation.getArguments()[0];
      int wrote = 0;
      for (int i = 0; i < values.length; i++) {
        if (i % 60 == 0 ) {
          if (i / 60 >= 10 && i / 60 < 25) {
            continue;
          }
          values[i] = val;
          ++wrote;
        }
      }
      return wrote;
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