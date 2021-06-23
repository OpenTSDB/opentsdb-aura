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
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class AuraMetricsSourceFactoryTest {

  private static MockTSDB TSDB;
  private static AuraMetricsSourceFactory FACTORY;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static QueryNode SINK;

  private QueryPipelineContext context;

  @BeforeClass
  public void beforeClass() {
    TSDB = new MockTSDB();
    FACTORY = new AuraMetricsSourceFactory(); // no need to init.
    FACTORY.setTimeSeriesRecordFactory(mock(TimeSeriesRecordFactory.class));
    FACTORY.setTimeSeriesStorage(mock(TimeSeriesStorageIf.class));
    SINK = mock(QueryNode.class);
    TSDB.registry = new DefaultRegistry(TSDB);
    (TSDB.registry).initialize(true);
    (TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, null, FACTORY);

    NUMERIC_CONFIG = (NumericInterpolatorConfig)
        NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NOT_A_NUMBER)
            .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
            .setDataType(NumericType.TYPE.toString())
            .build();

    QueryNodeConfig sink_config = mock(QueryNodeConfig.class);
    when(sink_config.getId()).thenReturn("SINK");
    when(SINK.config()).thenReturn(sink_config);
  }

  @BeforeMethod
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
  }

  @Test
  public void singleMetric() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build());

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();

    Assert.assertEquals(1, planner.sources().size());
    Assert.assertEquals(2, planner.graph().nodes().size());
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        SINK,
        planner.nodeForId("m1")));
  }

  @Test
  public void singleMetricDS() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .build());

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();

    Assert.assertEquals(1, planner.sources().size());
    Assert.assertEquals(2, planner.graph().nodes().size());
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        SINK,
        planner.nodeForId("m1")));
    QueryNode node = planner.nodeForId("m1");
    Assert.assertEquals(1, ((TimeSeriesDataSourceConfig) node.config()).getPushDownNodes().size());
    Assert.assertEquals("1m", ((DownsampleConfig) ((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(0)).getInterval());
  }

  @Test
  public void singleMetricDSGB() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();

    Assert.assertEquals(1, planner.sources().size());
    Assert.assertEquals(3, planner.graph().nodes().size());
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        SINK,
        planner.nodeForId("gb")));
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("gb"),
        planner.nodeForId("m1")));
    QueryNode node = planner.nodeForId("m1");
    Assert.assertEquals(1, ((TimeSeriesDataSourceConfig) node.config()).getPushDownNodes().size());
    Assert.assertEquals("1m", ((DownsampleConfig) ((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(0)).getInterval());
  }

  @Test
  public void singleMetricRateDSGB() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setId("m1")
            .build(),
        RateConfig.newBuilder()
            .setInterval("1s")
            .addSource("m1")
            .setId("rate")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("rate")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();

    Assert.assertEquals(1, planner.sources().size());
    Assert.assertEquals(3, planner.graph().nodes().size());
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        SINK,
        planner.nodeForId("gb")));
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("gb"),
        planner.nodeForId("m1")));
    QueryNode node = planner.nodeForId("m1");
    Assert.assertEquals(2, ((TimeSeriesDataSourceConfig) node.config()).getPushDownNodes().size());
    Assert.assertEquals("1s", ((RateConfig) ((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(0)).getInterval());
    Assert.assertEquals("1m", ((DownsampleConfig) ((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(1)).getInterval());
  }

  @Test
  public void singleMetricTimeShift() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setTimeShiftInterval("1h")
            .setId("m1")
            .build());

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();

    Assert.assertEquals(1, planner.sources().size());
    Assert.assertEquals(3, planner.graph().nodes().size());
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        SINK,
        planner.nodeForId("m1-timeShift")));
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("m1-timeShift"),
        planner.nodeForId("m1")));
  }

  @Test
  public void singleMetricTimeShiftDS() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setTimeShiftInterval("1h")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .build());

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();

    Assert.assertEquals(1, planner.sources().size());
    Assert.assertEquals(3, planner.graph().nodes().size());
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        SINK,
        planner.nodeForId("m1-timeShift")));
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("m1-timeShift"),
        planner.nodeForId("m1")));
    QueryNode node = planner.nodeForId("m1");
    Assert.assertEquals(1, ((TimeSeriesDataSourceConfig) node.config()).getPushDownNodes().size());
    Assert.assertEquals("1m", ((DownsampleConfig) ((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(0)).getInterval());
  }

  @Test
  public void singleMetricTimeShiftDSGB() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setTimeShiftInterval("1h")
            .setId("m1")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("m1")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();

    Assert.assertEquals(1, planner.sources().size());
    Assert.assertEquals(4, planner.graph().nodes().size());
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        SINK,
        planner.nodeForId("gb")));
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("gb"),
        planner.nodeForId("m1-timeShift")));
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("m1-timeShift"),
        planner.nodeForId("m1")));
    QueryNode node = planner.nodeForId("m1");
    Assert.assertEquals(1, ((TimeSeriesDataSourceConfig) node.config()).getPushDownNodes().size());
    Assert.assertEquals("1m", ((DownsampleConfig) ((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(0)).getInterval());
  }

  @Test
  public void singleMetricTimeShiftRateDSGB() throws Exception {
    List<QueryNodeConfig> graph = Lists.newArrayList(
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
            .setFilterId("f1")
            .setTimeShiftInterval("1h")
            .setId("m1")
            .build(),
        RateConfig.newBuilder()
            .setInterval("1s")
            .addSource("m1")
            .setId("rate")
            .build(),
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("downsample")
            .addSource("rate")
            .build(),
        GroupByConfig.newBuilder()
            .setAggregator("sum")
            .addTagKey("host")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("gb")
            .addSource("downsample")
            .build());

    SemanticQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1514764800")
        .setEnd("1514768400")
        .setExecutionGraph(graph)
        .build();
    when(context.query()).thenReturn(query);

    DefaultQueryPlanner planner =
        new DefaultQueryPlanner(context, SINK);
    planner.plan(null).join();

    Assert.assertEquals(1, planner.sources().size());
    Assert.assertEquals(4, planner.graph().nodes().size());
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        SINK,
        planner.nodeForId("gb")));
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("gb"),
        planner.nodeForId("m1-timeShift")));
    Assert.assertTrue(planner.graph().hasEdgeConnecting(
        planner.nodeForId("m1-timeShift"),
        planner.nodeForId("m1")));
    QueryNode node = planner.nodeForId("m1");
    Assert.assertEquals(2, ((TimeSeriesDataSourceConfig) node.config()).getPushDownNodes().size());
    Assert.assertEquals("1s", ((RateConfig) ((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(0)).getInterval());
    Assert.assertEquals("1m", ((DownsampleConfig) ((TimeSeriesDataSourceConfig) node.config())
        .getPushDownNodes().get(1)).getInterval());
  }

  @Test
  public void encodeJoinKeys() throws Exception {
    List<byte[]> keys = FACTORY.encodeJoinKeys(Lists.newArrayList("colo", "app"), null)
        .join();
    Assert.assertEquals(keys.size(), 2);
    assertArrayEquals("colo".getBytes(), keys.get(0));
    assertArrayEquals("app".getBytes(), keys.get(1));
  }

  @Test
  public void resolveByteId() throws Exception {
    BaseTimeSeriesByteId byte_id = BaseTimeSeriesByteId.newBuilder(
        mock(TimeSeriesDataSourceFactory.class))
        .setMetric("metric".getBytes())
        .addAggregatedTag("colo".getBytes())
        .addTags("app".getBytes(), "opentsdb".getBytes())
        .addTags("dept".getBytes(), "interior".getBytes())
        .build();

    TimeSeriesStringId id = FACTORY.resolveByteId(byte_id, null).join();
    Assert.assertEquals(id.metric(), "metric");
    Assert.assertEquals(id.aggregatedTags().size(), 1);
    Assert.assertEquals(id.aggregatedTags().get(0), "colo");
    Assert.assertEquals(id.tags().size(), 2);
    Assert.assertEquals(id.tags().get("app"), "opentsdb");
    Assert.assertEquals(id.tags().get("dept"), "interior");
  }
}
