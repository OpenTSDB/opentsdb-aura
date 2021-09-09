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

import static net.opentsdb.aura.metrics.core.downsample.AggregatorType.sum;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AggregatorTest {

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
  void countAggregator() {

    Aggregator aggregator = Aggregator.newBuilder(intervalCount).count().build();

    assertEquals(0b100, aggregator.getId());
    assertEquals("count", aggregator.getName());
    assertEquals(1, aggregator.getAggCount());

    apply(aggregator, rawData);

    double[] counts = aggregator.iterator().next();
    assertCountEquals(counts, intervalCount, intervalWidth);
  }

  @Test
  void sumAggregator() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).forType(sum).build();

    assertEquals(0b10, aggregator.getId());
    assertEquals("sum", aggregator.getName());
    assertEquals(1, aggregator.getAggCount());

    apply(aggregator, rawData);

    double[] sums = aggregator.iterator().next();
    assertSumEquals(sums, intervalCount, intervalWidth);
  }

  @Test
  void averageAggregator() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).forType("AVG").build();

    assertEquals(0b1, aggregator.getId());
    assertEquals("avg", aggregator.getName());
    assertEquals(1, aggregator.getAggCount());

    apply(aggregator, rawData);

    double[] averages = aggregator.iterator().next();
    assertAverageEquals(averages, intervalCount, intervalWidth);
  }

  @Test
  void minAggregator() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).min().build();

    assertEquals(0b1000, aggregator.getId());
    assertEquals("min", aggregator.getName());
    assertEquals(1, aggregator.getAggCount());

    apply(aggregator, rawData);

    double[] mins = aggregator.iterator().next();
    assertMinEquals(mins, intervalCount, intervalWidth);
  }

  @Test
  void minAggregatorWithNegativeValues() {
    Aggregator aggregator = Aggregator.newBuilder(3).min().build();

    double[] rawValues =
        new double[] {-7.8610975558955776E18, 4.739319893824937E18, -4.3908735418470799E18};
    aggregator.reset();
    for (int i = 0; i < rawValues.length; i++) {
      aggregator.apply(rawValues[i]);
      aggregator.accumulate(i);
    }

    double[] max = aggregator.iterator().next();
    assertArrayEquals(rawValues, max);
  }

  @Test
  void maxAggregator() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).max().build();

    assertEquals(0b10000, aggregator.getId());
    assertEquals("max", aggregator.getName());
    assertEquals(1, aggregator.getAggCount());

    apply(aggregator, rawData);

    double[] max = aggregator.iterator().next();
    assertMaxEquals(max, intervalCount, intervalWidth);
  }

  @Test
  void maxAggregatorWithNegativeValues() {
    Aggregator aggregator = Aggregator.newBuilder(3).max().build();

    double[] rawValues =
        new double[] {-7.8610975558955776E18, 4.739319893824937E18, -4.3908735418470799E18};
    aggregator.reset();
    for (int i = 0; i < rawValues.length; i++) {
      aggregator.apply(rawValues[i]);
      aggregator.accumulate(i);
    }

    double[] max = aggregator.iterator().next();
    assertArrayEquals(rawValues, max);
  }

  @Test
  void sumOfSquareAggregator() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).sumOfSquares().build();

    assertEquals(0b100000, aggregator.getId());
    assertEquals("sumofsquare", aggregator.getName());
    assertEquals(1, aggregator.getAggCount());

    apply(aggregator, rawData);

    double[] sumOfSquares = aggregator.iterator().next();
    assertSumOfSquareEquals(sumOfSquares, intervalCount, intervalWidth);
  }

  @Test
  void allAggregation() {
    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).avg().sum().count().min().max().sumOfSquares().build();

    assertEquals(0b111111, aggregator.getId());
    assertEquals("avg-sum-count-min-max-sumofsquare", aggregator.getName());
    assertEquals(6, aggregator.getAggCount());

    apply(aggregator, rawData);

    Iterator<double[]> iterator = aggregator.iterator();
    assertAverageEquals(iterator.next(), intervalCount, intervalWidth);
    assertSumEquals(iterator.next(), intervalCount, intervalWidth);
    assertCountEquals(iterator.next(), intervalCount, intervalWidth);
    assertMinEquals(iterator.next(), intervalCount, intervalWidth);
    assertMaxEquals(iterator.next(), intervalCount, intervalWidth);
    assertSumOfSquareEquals((iterator.next()), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void testOrderOfAggregators() {
    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).sumOfSquares().avg().max().min().count().sum().build();

    assertEquals(0b111111, aggregator.getId());
    assertEquals("sumofsquare-avg-max-min-count-sum", aggregator.getName());

    apply(aggregator, rawData);

    Iterator<double[]> iterator = aggregator.iterator();
    assertSumOfSquareEquals((iterator.next()), intervalCount, intervalWidth);
    assertAverageEquals(iterator.next(), intervalCount, intervalWidth);
    assertMaxEquals(iterator.next(), intervalCount, intervalWidth);
    assertMinEquals(iterator.next(), intervalCount, intervalWidth);
    assertCountEquals(iterator.next(), intervalCount, intervalWidth);
    assertSumEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void averageAndCount() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).avg().count().build();

    assertEquals(0b101, aggregator.getId());
    assertEquals("avg-count", aggregator.getName());
    assertEquals(2, aggregator.getAggCount());

    apply(aggregator, rawData);

    Iterator<double[]> iterator = aggregator.iterator();
    assertAverageEquals(iterator.next(), intervalCount, intervalWidth);
    assertCountEquals(iterator.next(), intervalCount, intervalWidth);

    assertFalse(iterator.hasNext());
  }

  @Test
  void duplicateAggregatorsNotSupported() {
    IllegalArgumentException expected =
        assertThrows(
            IllegalArgumentException.class,
            () -> Aggregator.newBuilder(intervalCount).sum().count().sum().build(),
            "Duplicate Aggregators should not be supported");
    assertEquals("Duplicate aggregator found for: sum", expected.getMessage());

    expected =
        assertThrows(
            IllegalArgumentException.class,
            () -> Aggregator.newBuilder(intervalCount).count().count().build(),
            "Duplicate Aggregators should not be supported");
    assertEquals("Duplicate aggregator found for: count", expected.getMessage());

    expected =
        assertThrows(
            IllegalArgumentException.class,
            () -> Aggregator.newBuilder(intervalCount).avg().count().sum().min().min().build(),
            "Duplicate Aggregators should not be supported");
    assertEquals("Duplicate aggregator found for: min", expected.getMessage());
  }

  @Test
  void reuseTheAggregator() {

    // reuse count
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).count().build();
    apply(aggregator, rawData);

    double[] rawData2 = new double[rawData.length];
    for (int i = 0; i < rawData2.length; i++) {
      rawData2[i] = i;
    }

    apply(aggregator, rawData2);
    Iterator<double[]> iterator = aggregator.iterator();
    double[] counts = iterator.next();
    assertEquals(counts.length, intervalCount);
    for (int i = 0; i < counts.length; i++) {
      assertEquals(intervalWidth, counts[i]);
    }
    assertFalse(iterator.hasNext());

    // reuse sum
    aggregator = Aggregator.newBuilder(intervalCount).sum().build();
    apply(aggregator, rawData);

    rawData2 = new double[rawData.length];
    for (int i = 0; i < rawData2.length; i++) {
      rawData2[i] = 1;
    }

    apply(aggregator, rawData2);
    iterator = aggregator.iterator();
    double[] sums = iterator.next();
    assertEquals(sums.length, intervalCount);
    for (int i = 0; i < sums.length; i++) {
      assertEquals(intervalWidth, sums[i]);
    }
    assertFalse(iterator.hasNext());

    // reuse average
    aggregator = Aggregator.newBuilder(intervalCount).avg().build();
    apply(aggregator, rawData);

    rawData2 = new double[rawData.length];
    for (int i = 0; i < rawData2.length; i++) {
      rawData2[i] = 1;
    }

    apply(aggregator, rawData2);
    iterator = aggregator.iterator();
    double[] averages = iterator.next();
    assertEquals(averages.length, intervalCount);
    for (int i = 0; i < averages.length; i++) {
      assertEquals(1, averages[i]);
    }
    assertFalse(iterator.hasNext());

    // reuse composite aggregator
    aggregator = Aggregator.newBuilder(intervalCount).avg().sum().count().build();
    apply(aggregator, rawData);

    rawData2 = new double[rawData.length];
    for (int i = 0; i < rawData2.length; i++) {
      rawData2[i] = 1;
    }

    apply(aggregator, rawData2);
    iterator = aggregator.iterator();
    averages = iterator.next();
    sums = iterator.next();
    counts = iterator.next();
    assertEquals(averages.length, intervalCount);
    assertEquals(sums.length, intervalCount);
    assertEquals(counts.length, intervalCount);
    for (int i = 0; i < averages.length; i++) {
      assertEquals(1, averages[i]);
      assertEquals(intervalWidth, sums[i]);
      assertEquals(intervalWidth, counts[i]);
    }
    assertFalse(iterator.hasNext());
  }

  private void apply(Aggregator aggregator, double[] rawData) {
    aggregator.reset();
    int offset = 0;
    boolean intervalHasValue = false;
    int lastIndex = rawData.length - 1;
    for (int i = 0; i <= lastIndex; i++) {
      double value = rawData[i];
      if (!Double.isNaN(value)) {
        aggregator.apply(value);
        if (!intervalHasValue) {
          intervalHasValue = true;
        }
      }

      offset++;
      if ((offset == intervalWidth || i == lastIndex)) {
        if (intervalHasValue) {
          int index = i / intervalWidth;
          aggregator.accumulate(index);
          intervalHasValue = false;
        }
        offset = 0;
      }
    }
  }

  static void generateRawData(double[] rawData, Interval interval) {
    int intervalWidth = interval.getWidth();
    for (int i = 0; i < rawData.length; i++) {
      int intervalOffset = i / intervalWidth % intervalWidth;
      int index = i % intervalWidth;
      if (index > 0 && index <= intervalOffset) {
        //  rawData[i] = index + (index) % 10 / 10.0;
        rawData[i] = index;
      } else {
        rawData[i] = Double.NaN;
      }
    }
  }

  static void assertSumOfSquareEquals(
      double[] sumOfSquares, short intervalCount, int intervalWidth) {
    assertEquals(intervalCount, sumOfSquares.length);

    double d = 0.0;
    for (int i = 0; i < sumOfSquares.length; i++) {
      int offset = i % intervalWidth;
      if (offset == 0) {
        assertTrue(Double.isNaN(sumOfSquares[i]));
        d = 0.0;
      } else {
        d += (offset * offset);
        assertEquals(d, sumOfSquares[i]);
      }
    }
  }

  static void assertMaxEquals(double[] max, short intervalCount, int intervalWidth) {
    assertEquals(intervalCount, max.length);

    for (int i = 0; i < max.length; i++) {
      int offset = i % intervalWidth;
      if (offset == 0) {
        assertTrue(Double.isNaN(max[i]));
      } else {
        assertEquals(offset, max[i]);
      }
    }
  }

  static void assertMinEquals(double[] mins, short intervalCount, int intervalWidth) {
    assertEquals(intervalCount, mins.length);

    for (int i = 0; i < mins.length; i++) {
      int offset = i % intervalWidth;
      if (offset == 0) {
        assertTrue(Double.isNaN(mins[i]));
      } else {
        assertEquals(1.0, mins[i]);
      }
    }
  }

  static void assertCountEquals(double[] counts, short intervalCount, int intervalWidth) {
    assertEquals(intervalCount, counts.length);

    for (int i = 0; i < counts.length; i++) {
      assertEquals(i % intervalWidth, counts[i]);
    }
  }

  static void assertSumEquals(double[] sums, short intervalCount, int intervalWidth) {
    assertEquals(intervalCount, sums.length);

    double v = 0.0;
    for (int i = 0; i < sums.length; i++) {
      int offset = i % intervalWidth;
      if (offset == 0) {
        assertTrue(Double.isNaN(sums[i]));
        v = 0.0;
      } else {
        v += offset;
        assertEquals(v, sums[i]);
      }
    }
  }

  static void assertAverageEquals(double[] averages, short intervalCount, int intervalWidth) {
    assertEquals(intervalCount, averages.length);
    double sum = 0.0;
    double count = 0.0;
    for (int i = 0; i < averages.length; i++) {
      int offset = i % intervalWidth;
      if (offset == 0) {
        assertTrue(Double.isNaN(averages[i]));
        sum = 0.0;
        count = 0.0;
      } else {
        sum += offset;
        count++;
        assertEquals(sum / count, averages[i]);
      }
    }
  }
}
