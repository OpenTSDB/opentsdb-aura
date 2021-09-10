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

import net.opentsdb.aura.metrics.core.RawTimeSeriesEncoder;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.utils.Pair;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AuraMetricsNumericArrayIteratorRateTest extends BaseAMNumericArrayTest {

  @BeforeClass
  public void beforeClassLocal() {
    setupRateData();
  }

  @Test
  public void alignedQunery60sNoDupes1Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
    }
  }

  @Test
  public void alignedQunery60sNoDupes1HourAsCount() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder().setInterval("1s").setRateToCount(true).setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], (100 * (i + 1) * 60), 0.001);
      }
    }
  }

  @Test
  public void alignedQunery60sNoDupes1HourAsCountWithInterval() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder()
            .setInterval("1s")
            .setInterval("10s")
            .setRateToCount(true)
            .setId("rate")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], (10 * (i + 1) * 60), 0.001);
      }
    }
  }

  @Test
  public void alignedQunery60sNoDupes1HourAsCount60s() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder().setInterval("60s").setRateToCount(true).setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], (100 * (i + 1)), 0.001);
      }
    }
  }

  @Test
  public void alignedQuery60sNoDupes2Hours120sRate() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 7200))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        (RateConfig) RateConfig.newBuilder().setInterval("120s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(120).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 120);
    for (int i = 0; i < 120; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], 200, 0.001);
      }
    }
  }

  @Test
  public void alignedQuery60sNoDupes24Hours() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + TimeUnit.HOURS.toSeconds(24)))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1440).build());
    long segmentTimes =
        storageContext.getSegmentTimes(base_ts, (int) (base_ts + TimeUnit.HOURS.toSeconds(24)));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60 * 24);
    for (int i = 0; i < 60 * 24; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN(v.value().doubleArray()[i]));
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
    }
  }

  @Test
  public void alignedQueryRunAllNoDupes1Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setRunAll(true)
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 1);
    assertEquals(v.value().doubleArray()[0], 98.333, 0.001);
  }

  @Test
  public void alignedQueryRunAllNoDupes2Hour() {
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setRunAll(true)
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 7200))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(1).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 7200);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 1);
    assertEquals(v.value().doubleArray()[0], 198.333, 0.001);
  }

  @Test
  public void alignedQuery60sShuffleNoDupes1Hours() {
    List<Pair<Integer, Double>> tss = new ArrayList<>();
    int ts = base_ts;
    int count = 0;
    while (ts < (base_ts + 3600)) {
      tss.add(new Pair<>(ts, (double) 100 * (count++ + 1)));
      ts += 60;
    }

    Collections.shuffle(tss);
    long segmentAddress = encoder.createSegment(base_ts);
    for (int i = 0; i < tss.size(); i++) {
      encoder.addDataPoint(tss.get(i).getKey(), tss.get(i).getValue());
    }

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate = RateConfig.newBuilder().setInterval("1s").setId("rate").build();

    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0) {
        assertTrue(Double.isNaN((v.value().doubleArray()[i])));
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
    }
  }

  // TODO - funky with the merged base class now.
//  @Test
//  public void alignedQuery60sDupes1Hours() {
//    DownsampleConfig config =
//        DownsampleConfig.newBuilder()
//            .setAggregator("sum")
//            .setInterval("1m")
//            .setStart(String.valueOf(base_ts))
//            .setEnd(String.valueOf(base_ts + 3600))
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setId("ds")
//            .build();
//
//    RateConfig rate =
//        RateConfig.newBuilder()
//            .setInterval("1s")
//            .setCounter(true)
//            .setDropResets(true)
//            .setId("rate")
//            .build();
//
//    RawTimeSeriesEncoder newEncoder = encoderFactory.create();
//    int segmentTime = segmentTimeList.get(0);
//    long segmentAddress = newEncoder.createSegment(segmentTime);
//
//    encoder.openSegment(segmentAddressList.get(0));
//    encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));
//
//    newEncoder.addDataPoint(base_ts, 100000.0);
//    newEncoder.addDataPoint(base_ts + 60, 1100000.0);
//    newEncoder.addDataPoint(base_ts + 120, 1100000.0);
//
//    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);
//
//    when(RESULT.aggregatorConfig())
//        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
//    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
//    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
//    int segmentCount = storageContext.getSegmentCount(segmentTimes);
//    AuraMetricsNumericArrayIterator iterator =
//        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
//
//    assertTrue(iterator.hasNext());
//
//    TimeSeriesValue<NumericArrayType> v = iterator.next();
//    assertEquals(v.value().doubleArray().length, 60);
//    for (int i = 0; i < 60; i++) {
//      if (i == 0 || i == 3) {
//        assertTrue(Double.isNaN((v.value().doubleArray()[i])));
//      } else if (i == 1) {
//        assertEquals(v.value().doubleArray()[i], 16666.666, 0.001);
//      } else if (i == 2) {
//        assertEquals(v.value().doubleArray()[i], 0.00, 0.001);
//      } else {
//        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
//      }
//    }
//  }

  @Test
  public void alignedQuery60sShuffleDupes1HoursCounter() {
    List<Pair<Integer, Double>> tss = new ArrayList<>();
    int ts = base_ts;
    int count = 0;
    while (ts < (base_ts + 3600)) {
      tss.add(new Pair<>(ts, (double) 100 * (count++ + 1)));
      ts += 60;
    }

    Collections.shuffle(tss);
    long segmentAddress = encoder.createSegment(base_ts);
    for (int i = 0; i < tss.size(); i++) {
      encoder.addDataPoint(tss.get(i).getKey(), tss.get(i).getValue());
    }

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder()
            .setInterval("1s")
            .setCounter(true)
            .setDropResets(true)
            .setId("rate")
            .build();

    // we always take latest here.
    encoder.addDataPoint(base_ts, 100000.0);
    encoder.addDataPoint(base_ts + 60, 100000.0);
    encoder.addDataPoint(base_ts + 120, 100000.0);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 60);
    for (int i = 0; i < 60; i++) {
      if (i == 0 || i == 3) {
        assertTrue(Double.isNaN((v.value().doubleArray()[i])));
      } else if (i == 1 || i == 2) {
        assertEquals(v.value().doubleArray()[i], 0.00, 0.001);
      } else {
        assertEquals(v.value().doubleArray()[i], 1.6666666666666667, 0.001);
      }
    }
  }

  @Test
  public void rate15mDownsample() {
    long segmentAddress = encoder.createSegment(base_ts);
    int ts = base_ts;
    int cnt = 0;
    for (int i = 0; i < 60; i++) {
      encoder.addDataPoint(ts, 100.0 * (cnt++ + 1));
      ts += 60;
    }

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval("15m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    RateConfig rate =
        RateConfig.newBuilder()
            .setInterval("1s")
            .setCounter(true)
            .setDropResets(true)
            .setId("rate")
            .build();
    when(RESULT.aggregatorConfig())
        .thenReturn(DefaultArrayAggregatorConfig.newBuilder().setArraySize(4).build());
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, rate, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);

    assertTrue(iterator.hasNext());

    TimeSeriesValue<NumericArrayType> v = iterator.next();
    assertEquals(v.value().doubleArray().length, 4);
    for (int i = 0; i < 4; i++) {
      if (i == 0) {
        assertEquals(v.value().doubleArray()[i], 23.333, 0.001);
      } else {
        assertEquals(v.value().doubleArray()[i], 25.000, 0.001);
      }
    }
  }
}
