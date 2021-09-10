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
import net.opentsdb.aura.metrics.core.ResultConsumer;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.meta.MetricQuery;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuraMetricsQueryNodeTest {
  private static final int BASE_TS = 1617235200;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static MockTSDB TSDB;
  private static QueryPipelineContext CONTEXT;

  private AuraMetricsSourceFactory factory;
  private TimeSeriesStorageIf storage;
  private TimeSeriesRecordFactory recordFactory;
  private QueryNode upstream;

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

    CONTEXT = mock(QueryPipelineContext.class);
    when(CONTEXT.tsdb()).thenReturn(TSDB);
  }

  @BeforeMethod
  public void before() {
    factory = new AuraMetricsSourceFactory();
    storage = mock(TimeSeriesStorageIf.class);
    recordFactory = mock(TimeSeriesRecordFactory.class);
    factory.setTimeSeriesStorage(storage);
    factory.setTimeSeriesRecordFactory(recordFactory);
    upstream = mock(QueryNode.class);
    when(CONTEXT.upstream(any(QueryNode.class)))
            .thenReturn(Lists.newArrayList(upstream));
  }

  @Test
  public void fetchNext() throws Exception {
    TimeSeriesDataSourceConfig config = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.busy")
                    .build())
            .setStartTimeStamp(new SecondTimeStamp(BASE_TS))
            .setEndTimeStamp(new SecondTimeStamp(BASE_TS + 3600))
            .setId("m1")
            .build();
    AuraMetricsQueryNode node = factory.newNode(CONTEXT, config);
    node.initialize(null).join();
    node.fetchNext(null);

    verify(storage, times(1))
            .read(any(MetricQuery.class),
                  eq(BASE_TS),
                  eq(BASE_TS + 3600),
                  any(ResultConsumer.class));
    verify(upstream, times(1)).onNext(any(AuraMetricsQueryResult.class));
  }

  @Test
  public void fetchNextTimeShift() throws Exception {
    TimeSeriesDataSourceConfig config = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.busy")
                    .build())
            .setStartTimeStamp(new SecondTimeStamp(BASE_TS))
            .setEndTimeStamp(new SecondTimeStamp(BASE_TS + 3600))
            .setTimeShiftInterval("1h")
            .setId("m1")
            .build();
    AuraMetricsQueryNode node = factory.newNode(CONTEXT, config);
    node.initialize(null).join();
    node.fetchNext(null);

    verify(storage, times(1))
            .read(any(MetricQuery.class),
                    eq(BASE_TS - 3600),
                    eq(BASE_TS),
                    any(ResultConsumer.class));
    verify(upstream, times(1)).onNext(any(AuraMetricsQueryResult.class));
  }
}
