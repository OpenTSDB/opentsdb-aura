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

package net.opentsdb.aura.metrics.core.downsample;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static net.opentsdb.aura.metrics.core.downsample.AggregatorTest.assertAverageEquals;
import static net.opentsdb.aura.metrics.core.downsample.AggregatorTest.assertCountEquals;
import static net.opentsdb.aura.metrics.core.downsample.AggregatorTest.assertMaxEquals;
import static net.opentsdb.aura.metrics.core.downsample.AggregatorTest.assertMinEquals;
import static net.opentsdb.aura.metrics.core.downsample.AggregatorTest.assertSumEquals;
import static net.opentsdb.aura.metrics.core.downsample.AggregatorTest.assertSumOfSquareEquals;
import static net.opentsdb.aura.metrics.core.downsample.AggregatorTest.generateRawData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class DownSamplerTest {

  private static Interval interval = Interval._30_SEC;
  private static SegmentWidth segmentWidth = SegmentWidth._2_HR;
  private static int intervalWidth = interval.getWidth();
  private static short intervalCount = interval.getCount(segmentWidth);
  private static double[] rawData = new double[segmentWidth.getWidth()];

  @BeforeAll
  static void beforeAll() {
    generateRawData(rawData, interval);
  }

  @Test
  void downSampleSum() {
    Aggregator aggr = Aggregator.newBuilder(intervalCount).sum().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggr);
    downSampler.apply(rawData);
    Iterator<double[]> iterator = downSampler.iterator();
    assertSumEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void downSampleCount() {
    Aggregator aggr = Aggregator.newBuilder(intervalCount).count().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggr);
    downSampler.apply(rawData);
    Iterator<double[]> iterator = downSampler.iterator();
    assertCountEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void downSampleAverage() {
    Aggregator aggr = Aggregator.newBuilder(intervalCount).avg().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggr);
    downSampler.apply(rawData);
    Iterator<double[]> iterator = downSampler.iterator();
    assertAverageEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void downSampleMin() {
    Aggregator aggr = Aggregator.newBuilder(intervalCount).min().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggr);
    downSampler.apply(rawData);
    Iterator<double[]> iterator = downSampler.iterator();
    assertMinEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void downSampleMax() {
    Aggregator aggr = Aggregator.newBuilder(intervalCount).max().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggr);
    downSampler.apply(rawData);
    Iterator<double[]> iterator = downSampler.iterator();
    assertMaxEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void downSampleSumOfSquares() {
    Aggregator aggr = Aggregator.newBuilder(intervalCount).sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggr);
    downSampler.apply(rawData);
    Iterator<double[]> iterator = downSampler.iterator();
    assertSumOfSquareEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void downSampleAverageAndCount() {
    Aggregator aggr = Aggregator.newBuilder(intervalCount).avg().count().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggr);
    downSampler.apply(rawData);
    Iterator<double[]> iterator = downSampler.iterator();
    assertAverageEquals(iterator.next(), intervalCount, intervalWidth);
    assertCountEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void downSampleAllAggregations() {
    Aggregator aggr =
        Aggregator.newBuilder(intervalCount).avg().sum().count().min().max().sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggr);
    downSampler.apply(rawData);
    AggregatorIterator<double[]> iterator = downSampler.iterator();

    assertAverageEquals(iterator.next(), intervalCount, intervalWidth);
    assertEquals(AverageAggregator.ID, iterator.aggID());
    assertEquals(AverageAggregator.NAME, iterator.aggName());

    assertSumEquals(iterator.next(), intervalCount, intervalWidth);
    assertEquals(SumAggregator.ID, iterator.aggID());
    assertEquals(SumAggregator.NAME, iterator.aggName());

    assertCountEquals(iterator.next(), intervalCount, intervalWidth);
    assertEquals(CountAggregator.ID, iterator.aggID());
    assertEquals(CountAggregator.NAME, iterator.aggName());

    assertMinEquals(iterator.next(), intervalCount, intervalWidth);
    assertEquals(MinAggregator.ID, iterator.aggID());
    assertEquals(MinAggregator.NAME, iterator.aggName());

    assertMaxEquals(iterator.next(), intervalCount, intervalWidth);
    assertEquals(MaxAggregator.ID, iterator.aggID());
    assertEquals(MaxAggregator.NAME, iterator.aggName());

    assertSumOfSquareEquals(iterator.next(), intervalCount, intervalWidth);
    assertEquals(SumOfSquareAggregator.ID, iterator.aggID());
    assertEquals(SumOfSquareAggregator.NAME, iterator.aggName());

    assertFalse(iterator.hasNext());
  }
}
