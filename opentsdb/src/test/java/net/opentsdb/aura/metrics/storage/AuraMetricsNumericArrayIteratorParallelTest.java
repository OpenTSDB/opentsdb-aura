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
import net.opentsdb.data.types.numeric.aggregators.ArrayAverageFactory;
import net.opentsdb.data.types.numeric.aggregators.ArraySumFactory;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AuraMetricsNumericArrayIteratorParallelTest extends BaseAMNumericArrayTest {
  private static ArraySumFactory FACTORY;

  @BeforeClass
  public void beforeClassLocal() {
    setupConstantData(5.0);
    FACTORY = new ArraySumFactory();
  }

  @Test
  public void testDownSample() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval + "m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[60 / interval];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();
    assertEquals(
        downSampled,
        new double[] {25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0});
  }

  @Test
  public void testDownSampleIntervalNotAlignedToSegment() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval(interval + "m")
            .setStart(String.valueOf((base_ts + 322)))
            .setEnd(String.valueOf((base_ts + (3600 - 685))))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    long segmentTimes = storageContext.getSegmentTimes(base_ts + 322, base_ts + (3600 - 685));
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[config.intervals()];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();
    assertEquals(downSampled, new double[] {5, 5, 5.0, 5.0, 5.0, 5.0, 5.0});
  }

  @Test
  public void testSkipSegmentsBeforeTheStartTime() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval + "m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    List<Integer> segTimes = new ArrayList<>();
    List<Long> segAddresses = new ArrayList<>();

    int segmentTime = base_ts - (3600 * 2);
    segTimes.add(segmentTime);
    long segmentAddress = encoder.createSegment(segmentTime);
    segAddresses.add(segmentAddress);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);

    segmentTime = base_ts - 3600;
    segTimes.add(segmentTime);
    segmentAddress = encoder.createSegment(segmentTime);
    segAddresses.add(segmentAddress);

    timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddress);

    RawTimeSeriesEncoder newEncoder = encoderFactory.create();
    for (int i = 0; i < segmentAddressList.size(); i++) {
      encoder.openSegment(segmentAddressList.get(i));
      int segTime = segmentTimeList.get(i);
      long segAddress = newEncoder.createSegment(segTime);
      encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));
      timeSeriesRecord.setSegmentAddress(segTime, segAddress);
      segTimes.add(segTime);
      segAddresses.add(segAddress);
    }

    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(
            config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[60 / interval];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();
    assertEquals(
        downSampled,
        new double[] {25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0});
  }

  @Test
  public void testSkipSegmentsAfterTheEndTime() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval + "m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    List<Integer> segmentTimeList = new ArrayList<>();
    List<Long> segmentAddresses = new ArrayList<>();
    long tsPointer = 0;
    RawTimeSeriesEncoder newEncoder = encoderFactory.create();
    for (int i = 0; i < AuraMetricsNumericArrayIteratorParallelTest.segmentTimeList.size(); i++) {
      encoder.openSegment(segmentAddressList.get(i));

      int segmentTime = AuraMetricsNumericArrayIteratorParallelTest.segmentTimeList.get(i);
      long segmentAddress = newEncoder.createSegment(segmentTime);
      segmentAddresses.add(segmentAddress);
      segmentTimeList.add(segmentTime);
      encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));

      if (i == 0) {
        tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);
      } else {
        timeSeriesRecord.setSegmentAddress(segmentTime, segmentAddress);
      }
    }

    int segmentTime = base_ts + 3600;
    segmentTimeList.add(segmentTime);
    long segmentAddress = encoder.createSegment(segmentTime);
    segmentAddresses.add(segmentAddress);

    segmentTime = base_ts + (3600 * 2);
    segmentTimeList.add(segmentTime);
    segmentAddress = encoder.createSegment(segmentTime);
    segmentAddresses.add(segmentAddress);

    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(
            config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[60 / interval];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();
    assertEquals(
        downSampled,
        new double[] {25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0, 25.0});
  }

  @Test
  public void testSkipIntervalsBeforeTheStartTime() {
    int interval = 5;
    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("sum")
            .setInterval(interval + "m")
            .setStart(String.valueOf(base_ts - 1800))
            .setEnd(String.valueOf(base_ts + 1800))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();

    List<Integer> segmentTimeList = new ArrayList<>();
    List<Long> segmentAddresses = new ArrayList<>();
    int segmentTime = base_ts - 3600;
    segmentTimeList.add(segmentTime);
    long segmentAddress = encoder.createSegment(segmentTime);
    segmentAddresses.add(segmentAddress);

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, segmentTime, segmentAddress);

    RawTimeSeriesEncoder newEncoder = encoderFactory.create();
    for (int i = 0; i < segmentAddressList.size(); i++) {
      int segTime = AuraMetricsNumericArrayIteratorParallelTest.segmentTimeList.get(i);
      encoder.openSegment(segmentAddressList.get(i));
      long segAddress = newEncoder.createSegment(segTime);
      encoder.read((t, v, d) -> newEncoder.addDataPoint(t, v));
      timeSeriesRecord.setSegmentAddress(segTime, segAddress);
    }

    long segmentTimes = storageContext.getSegmentTimes(base_ts - 1800, base_ts + 1800);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(
            config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArraySumFactory.ArraySum aggregator = new ArraySumFactory.ArraySum(false, FACTORY);
    double[] nans = new double[60 / interval];
    java.util.Arrays.fill(nans, Double.NaN);
    aggregator.accumulate(nans);

    iterator.next(aggregator);
    double[] downSampled = aggregator.doubleArray();

    int i = 0;
    for (; i < 6; i++) {
      assertTrue(Double.isNaN(downSampled[i]));
    }
    for (; i < 12; i++) {
      assertEquals(downSampled[i], 25.0);
    }
  }

  @Test
  public void testDownSampleAvgGroupByAvg() {
    long segmentAddress = encoder.createSegment(base_ts);

    double[] values = new double[] {1.0, 5.0, 10.0};
    encoder.addDataPoint(base_ts, values[0]);

    for (int i = 1; i < 360; i++) {
      encoder.addDataPoint((base_ts + (10 * i)), values[i % 3]);
    }

    long tsPointer = timeSeriesRecord.create(0, 0, (byte) 0, 0, 0.0, base_ts, segmentAddress);

    DownsampleConfig config =
        DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("1m")
            .setStart(String.valueOf(base_ts))
            .setEnd(String.valueOf(base_ts + 3600))
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setId("ds")
            .build();
    long segmentTimes = storageContext.getSegmentTimes(base_ts, base_ts + 3600);
    int firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
    int segmentCount = storageContext.getSegmentCount(segmentTimes);
    AuraMetricsNumericArrayIterator iterator =
        new AuraMetricsNumericArrayIterator(config, null, NODE, RESULT, tsPointer, firstSegmentTime, segmentCount);
    assertTrue(iterator.hasNext());

    ArrayAverageFactory.ArrayAverage dsAggregator =
        new ArrayAverageFactory.ArrayAverage(false, new ArrayAverageFactory());
    double[] nans = new double[60];
    java.util.Arrays.fill(nans, Double.NaN);
    dsAggregator.accumulate(nans);

    iterator.next(dsAggregator);

    ArrayAverageFactory.ArrayAverage groupByAggregator =
        new ArrayAverageFactory.ArrayAverage(false, new ArrayAverageFactory());
    groupByAggregator.combine(dsAggregator);
    groupByAggregator.combine(dsAggregator);

    double[] result = groupByAggregator.doubleArray();
    assertEquals(result.length, 60);
    for (int i = 0; i < result.length; i++) {
      assertEquals(result[i], 5.333333333333333);
    }
  }
}
