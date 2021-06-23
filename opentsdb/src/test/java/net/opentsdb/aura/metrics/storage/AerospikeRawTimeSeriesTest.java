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
import net.opentsdb.aura.metrics.core.LongTermStorage;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoder;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class AerospikeRawTimeSeriesTest {

  static TimeSeriesDataSourceConfig timeSeriesDataSourceConfig;
  AerospikeQueryResult result;
  AerospikeQueryNode node;
  QueryPipelineContext context;
  MetaTimeSeriesQueryResult.GroupResult gr;
  LTSAerospike asClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    timeSeriesDataSourceConfig = DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric("sys.cpu.usr")
                    .build())
            .setId("m1")
            .build();
  }

  @BeforeMethod
  public void before() throws Exception {
    result = mock(AerospikeQueryResult.class);
    node = mock(AerospikeQueryNode.class);
    context = mock(QueryPipelineContext.class);
    gr = mock(MetaTimeSeriesQueryResult.GroupResult.class);
    asClient = mock(LTSAerospike.class);
    when(node.getSegmentReadArray()).thenReturn(new double[3600 * 2]);

    when(result.source()).thenReturn(node);
    when(node.pipelineContext()).thenReturn(context);
    when(node.asClient()).thenReturn(asClient);
  }

  @Test
  public void noData() throws Exception {
    SemanticQuery query =
            SemanticQuery.newBuilder()
                    .setMode(QueryMode.SINGLE)
                    .addExecutionGraphNode(timeSeriesDataSourceConfig)
                    .setStart("1614556800")
                    .setEnd("1614578400")
                    .build();
    when(context.query()).thenReturn(query);
    when(node.getSegmentsStart()).thenReturn(1614556800);
    when(node.getSegmentsEnd()).thenReturn(1614556800 + (3600 * 6));

    LongTermStorage.Records records = mock(LongTermStorage.Records.class);
    when(asClient.read(anyLong(), anyInt(), anyInt())).thenReturn(records);
    AerospikeRawTimeSeries ts = new AerospikeRawTimeSeries(result, gr);
    TypedTimeSeriesIterator<NumericType> it = (TypedTimeSeriesIterator<NumericType>) ts.iterator(NumericType.TYPE).get();
    assertFalse(it.hasNext());
  }

  @Test
  public void fullSegments() throws Exception {
    SemanticQuery query =
            SemanticQuery.newBuilder()
                    .setMode(QueryMode.SINGLE)
                    .addExecutionGraphNode(timeSeriesDataSourceConfig)
                    .setStart("1614556800")
                    .setEnd("1614578400")
                    .build();
    when(context.query()).thenReturn(query);
    when(node.getSegmentsStart()).thenReturn(1614556800);
    when(node.getSegmentsEnd()).thenReturn(1614556800 + (3600 * 6));

    LongTermStorage.Records records = mock(LongTermStorage.Records.class);
    when(asClient.read(anyLong(), anyInt(), anyInt())).thenReturn(records);
    when(records.hasNext())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
    TimeSeriesEncoder enc1 = mockEncoder(1614556800);
    TimeSeriesEncoder enc2 = mockEncoder(1614556800 + (3600 * 2));
    TimeSeriesEncoder enc3 = mockEncoder(1614556800 + (3600 * 4));
    when(records.next())
            .thenReturn(enc1)
            .thenReturn(enc2)
            .thenReturn(enc3);

    AerospikeRawTimeSeries ts = new AerospikeRawTimeSeries(result, gr);
    TypedTimeSeriesIterator<NumericType> it = (TypedTimeSeriesIterator<NumericType>) ts.iterator(NumericType.TYPE).get();
    assertTrue(it.hasNext());
    int timestamp = 1614556800;
    double val = 0;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> value = it.next();
      assertEquals(value.timestamp().epoch(), timestamp);
      assertEquals(value.value().doubleValue(), val, 0.0001);

      timestamp += 300;
      if (timestamp % (3600 * 2) == 0) {
        val = 0;
      } else {
        val += 300;
      }
    }
  }

  @Test
  public void gapInMiddle() throws Exception {
    SemanticQuery query =
            SemanticQuery.newBuilder()
                    .setMode(QueryMode.SINGLE)
                    .addExecutionGraphNode(timeSeriesDataSourceConfig)
                    .setStart("1614556800")
                    .setEnd("1614578400")
                    .build();
    when(context.query()).thenReturn(query);
    when(node.getSegmentsStart()).thenReturn(1614556800);
    when(node.getSegmentsEnd()).thenReturn(1614556800 + (3600 * 6));

    LongTermStorage.Records records = mock(LongTermStorage.Records.class);
    when(asClient.read(anyLong(), anyInt(), anyInt())).thenReturn(records);
    when(records.hasNext())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
    TimeSeriesEncoder enc1 = mockEncoder(1614556800);
    //TimeSeriesEncoder enc2 = mockEncoder(1614556800 + (3600 * 2));
    TimeSeriesEncoder enc3 = mockEncoder(1614556800 + (3600 * 4));
    when(records.next())
            .thenReturn(enc1)
            //.thenReturn(enc2)
            .thenReturn(enc3);

    AerospikeRawTimeSeries ts = new AerospikeRawTimeSeries(result, gr);
    TypedTimeSeriesIterator<NumericType> it = (TypedTimeSeriesIterator<NumericType>) ts.iterator(NumericType.TYPE).get();
    assertTrue(it.hasNext());
    int timestamp = 1614556800;
    double val = 0;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> value = it.next();
      assertEquals(value.timestamp().epoch(), timestamp);
      assertEquals(value.value().doubleValue(), val, 0.0001);

      timestamp += 300;
      if (timestamp == 1614564000) {
        timestamp = 1614571200;
        val = 0;
      } else {
        val += 300;
      }
    }
  }

  TimeSeriesEncoder mockEncoder(int segmentTime) {
    TimeSeriesEncoder encoder = mock(TimeSeriesEncoder.class);
    when(encoder.getSegmentTime()).thenReturn(segmentTime);
    when(encoder.readAndDedupe(any(double[].class))).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        double[] values = (double[]) invocation.getArguments()[0];
        int wrote = 0;
        for (int i = 0; i < values.length; i++) {
          if (i % 300 == 0) {
            values[i] = i;
            ++wrote;
          }
        }
        return wrote;
      }
    });
    return encoder;
  }

}