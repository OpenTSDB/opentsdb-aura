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

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AuraMetricsNumericArrayIteratorShiftTest extends BaseAMNumericArrayTest {

  @BeforeClass
  public void beforeClassLocal() {
    setupTimestampData();
  }

  @Test
  public void alignedQuery60sShift1h() {
    DownsampleConfig config =
            DownsampleConfig.newBuilder()
                    .setAggregator("sum")
                    .setInterval("1m")
                    .setStart(String.valueOf(base_ts + 3600))
                    .setEnd(String.valueOf(base_ts + (3600 * 2)))
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .setId("ds")
                    .build();
    when(NODE.shiftSeconds()).thenReturn(3600);
    when(RESULT.aggregatorConfig())
            .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
            new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.timestamp().epoch(), base_ts + 3600);
    assertEquals(v.value().doubleArray().length, 60);
    int i = 0;
    int val = base_ts;
    for (; i < 60; i++) {
      assertEquals(v.value().doubleArray()[i], val, 0.001);
      val += 60;
    }
    assertEquals(60, i);
  }

  @Test
  public void alignedQuery60sShift2h() {
    DownsampleConfig config =
            DownsampleConfig.newBuilder()
                    .setAggregator("sum")
                    .setInterval("1m")
                    .setStart(String.valueOf(base_ts + (3600 * 2)))
                    .setEnd(String.valueOf(base_ts + (3600 * 3)))
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .setId("ds")
                    .build();
    when(NODE.shiftSeconds()).thenReturn(3600 * 2);
    when(RESULT.aggregatorConfig())
            .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
            new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.timestamp().epoch(), base_ts + (3600 * 2));
    assertEquals(v.value().doubleArray().length, 60);
    int i = 0;
    int val = base_ts;
    for (; i < 60; i++) {
      assertEquals(v.value().doubleArray()[i], val, 0.001);
      val += 60;
    }
    assertEquals(60, i);
  }

  @Test
  public void alignedQuery60sShift1hOORange() {
    DownsampleConfig config =
            DownsampleConfig.newBuilder()
                    .setAggregator("sum")
                    .setInterval("1m")
                    .setStart(String.valueOf(base_ts + 3600))
                    .setEnd(String.valueOf(base_ts + (3600 * 2)))
                    .addInterpolatorConfig(NUMERIC_CONFIG)
                    .setId("ds")
                    .build();
    when(NODE.shiftSeconds()).thenReturn(3600 * 2);
    when(RESULT.aggregatorConfig())
            .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
            new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.timestamp().epoch(), base_ts + 3600);
    assertEquals(v.value().doubleArray().length, 60);
    int i = 0;
    int val = base_ts;
    for (; i < 60; i++) {
      assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      val += 60;
    }
    assertEquals(60, i);
  }

}
