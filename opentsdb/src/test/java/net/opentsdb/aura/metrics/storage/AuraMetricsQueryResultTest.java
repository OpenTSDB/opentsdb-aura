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

import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.core.data.ResultantPointerArray;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class AuraMetricsQueryResultTest {
  private static NumericInterpolatorConfig NUMERIC_CONFIG =
      (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
          .setFillPolicy(FillPolicy.NOT_A_NUMBER)
          .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
          .setDataType(NumericType.TYPE.toString())
          .build();
  private static final String namespace = "test";
  private static AuraMetricsQueryNode NODE;

  @BeforeClass
  public static void before() {
    NODE = mock(AuraMetricsQueryNode.class);
    AuraMetricsSourceFactory SOURCE_FACTORY = new AuraMetricsSourceFactory();
    TimeSeriesStorageIf TIME_SERIES_STORAGE = mock(TimeSeriesStorageIf.class);
    when(TIME_SERIES_STORAGE.secondsInSegment()).thenReturn(3600);
    SOURCE_FACTORY.setTimeSeriesStorage(TIME_SERIES_STORAGE);
    when(NODE.factory()).thenReturn(SOURCE_FACTORY);
    when(TIME_SERIES_STORAGE.numShards()).thenReturn(10);
  }

  @Test
  public void noPushDowns() throws Exception {
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(NODE.pipelineContext()).thenReturn(context);
    TSDB tsdb = mock(TSDB.class);
    when(context.tsdb()).thenReturn(tsdb);
    Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);

    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user").build())
            .setId("foo")
            .build();
    when(NODE.config()).thenReturn(config);

    AuraMetricsQueryResult result = new AuraMetricsQueryResult(NODE, null);
    assertSame(result.queryNode, NODE);
    assertNull(result.downsampleConfig());
    assertNull(result.rateConfig());

    ResultantPointerArray a1 = new ResultantPointerArray(1);
    a1.setTSCount(1);
    ResultantPointerArray a2 = new ResultantPointerArray(1);
    a2.setTSCount(1);
    result.addAllTimeSeries(0, a1.getAddress());
    result.addAllTimeSeries(1, a2.getAddress());
    result.finished();

    List<TimeSeries> iterators = result.timeSeries();
   assertEquals(iterators.size(), 2);
  }

  @Test
  public void DownsamplePushDown() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    when(NODE.config()).thenReturn(config);
    AuraMetricsQueryResult result = new AuraMetricsQueryResult(NODE, null);
    assertSame(result.queryNode, NODE);
    assertSame(result.downsampleConfig(), config.getPushDownNodes().get(0));
    assertNull(result.rateConfig());
  }

  @Test
  public void RatePushDown() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user").build())
            .addPushDownNode(RateConfig.newBuilder()
                .setInterval("1s")
                .setId("rate")
                .build())
            .setId("foo")
            .build();
    when(NODE.config()).thenReturn(config);
    AuraMetricsQueryResult result = new AuraMetricsQueryResult(NODE, null);
    assertSame(result.queryNode, NODE);
    assertNull(result.downsampleConfig());
    assertSame(result.rateConfig(), config.getPushDownNodes().get(0));
  }

  @Test
  public void DownsampleAndRatePushDown() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
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
    when(NODE.config()).thenReturn(config);
    AuraMetricsQueryResult result = new AuraMetricsQueryResult(NODE, null);
    assertSame(result.queryNode, NODE);
    assertSame(result.downsampleConfig(), config.getPushDownNodes().get(0));
    assertSame(result.rateConfig(), config.getPushDownNodes().get(1));
  }

}
