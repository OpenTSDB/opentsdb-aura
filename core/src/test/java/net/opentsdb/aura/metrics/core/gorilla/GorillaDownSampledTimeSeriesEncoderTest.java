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

package net.opentsdb.aura.metrics.core.gorilla;

import net.opentsdb.aura.metrics.core.TimeSeriesEncoderType;
import net.opentsdb.aura.metrics.core.downsample.AggregationLengthIterator;
import net.opentsdb.aura.metrics.core.downsample.Aggregator;
import net.opentsdb.aura.metrics.core.downsample.AggregatorIterator;
import net.opentsdb.aura.metrics.core.downsample.AverageAggregator;
import net.opentsdb.aura.metrics.core.downsample.CountAggregator;
import net.opentsdb.aura.metrics.core.downsample.DownSampler;
import net.opentsdb.aura.metrics.core.downsample.Interval;
import net.opentsdb.aura.metrics.core.downsample.MaxAggregator;
import net.opentsdb.aura.metrics.core.downsample.MinAggregator;
import net.opentsdb.aura.metrics.core.downsample.SegmentWidth;
import net.opentsdb.aura.metrics.core.downsample.SumAggregator;
import net.opentsdb.aura.metrics.core.downsample.SumOfSquareAggregator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static net.opentsdb.aura.metrics.core.TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS;
import static net.opentsdb.aura.metrics.core.downsample.DownSampledTimeSeriesEncoder.encodeInterval;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class GorillaDownSampledTimeSeriesEncoderTest {

  private static final long LOSS_MASK = 0xFFFFFFFFFF000000l;
  private static final int SEGMENT_TIMESTAMP = 1611288000;

  private static double[] values =
      new double[] {
        0.8973950653568595,
        0.43654855440361706,
        0.827450779634358,
        0.3584920510780665,
        0.9295624657514724,
        0.9610921547553934,
        0.6329804921575314,
        0.34905996592724153,
        0.5379730703355181,
        0.8403559626106764,
        0.30075147324566376,
        0.15691026481149195,
        0.7525354276869367,
        0.942970394430076,
        0.2401190623680185,
        0.42611404794594654,
        0.7615746658524079,
        0.46418976228229414,
        0.6942765189361159,
        0.9690728790734268,
        0.32890435244089244,
        0.6098703276841767,
        0.22878432168195317,
        0.8844305249065624,
        0.7157591580282211
      };

  private static Interval interval = Interval._30_SEC;
  private static SegmentWidth segmentWidth = SegmentWidth._2_HR;
  private static int intervalWidth = interval.getSeconds();
  private static short intervalCount = interval.getCount(segmentWidth);
  private static double[] rawValues = new double[segmentWidth.getWidth()];
  private static Random random = new Random(System.currentTimeMillis());

  private OffHeapGorillaDownSampledSegment segment =
      new OffHeapGorillaDownSampledSegment(256);

  private GorillaDownSampledTimeSeriesEncoder encoder;

  @BeforeAll
  static void beforeAl() {
    Arrays.fill(rawValues, Double.NaN);
    for (int i = 0; i < rawValues.length; i++) {
      if (i % intervalWidth == 0) {
        rawValues[i] = values[i % values.length];
      }
    }
  }

  @Test
  void addAverage() {

    Aggregator aggregator = Aggregator.newBuilder(intervalCount).avg().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    assertEquals(1, encoder.getAggCount());
    assertEquals(intervalCount, encoder.getIntervalCount());

    downSampler.apply(rawValues);

    Iterator<double[]> iterator = downSampler.iterator();
    double[] expectedAvg = iterator.next();

    encoder.createSegment(SEGMENT_TIMESTAMP);

    encoder.addDataPoints(rawValues);

    assertEquals(intervalCount, encoder.getNumDataPoints());

    AggregationLengthIterator aggLengthIterator = encoder.aggIterator();
    aggLengthIterator.next();
    assertEquals(16224, aggLengthIterator.aggLengthInBits());
    assertEquals(AverageAggregator.ID, aggLengthIterator.aggID());
    assertEquals(AverageAggregator.NAME, aggLengthIterator.aggName());
    assertFalse(aggLengthIterator.hasNext());

    double[] aggValues = new double[expectedAvg.length];

    encoder.readAggValues(aggValues, aggregator.getId());

    assertArrayEquals(expectedAvg, aggValues);
  }

  @Test
  void addAverageAndCount() {

    Aggregator aggregator = Aggregator.newBuilder(intervalCount).avg().count().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    assertEquals(2, encoder.getAggCount());
    assertEquals(intervalCount, encoder.getIntervalCount());

    downSampler.apply(rawValues);

    Iterator<double[]> iterator = downSampler.iterator();
    double[] expectedAvgs = iterator.next();
    double[] expectedCounts = iterator.next();

    long address = encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(rawValues);

    assertEquals(intervalCount, encoder.getNumDataPoints());

    AggregationLengthIterator aggLengthIterator = encoder.aggIterator();
    aggLengthIterator.next();
    assertEquals(16224, aggLengthIterator.aggLengthInBits());
    assertEquals(AverageAggregator.ID, aggLengthIterator.aggID());
    assertEquals(AverageAggregator.NAME, aggLengthIterator.aggName());

    double[] aggValues = new double[expectedAvgs.length];
    encoder.readAggValues(aggValues, aggLengthIterator.aggID());
    assertArrayEquals(expectedAvgs, aggValues);

    aggLengthIterator.next();
    assertEquals(304, aggLengthIterator.aggLengthInBits());
    assertEquals(CountAggregator.ID, aggLengthIterator.aggID());
    assertEquals(CountAggregator.NAME, aggLengthIterator.aggName());

    encoder.openSegment(address);

    double[] countValues = new double[expectedCounts.length];
    encoder.readAggValues(countValues, aggLengthIterator.aggID());
    assertArrayEquals(expectedCounts, countValues);

    assertFalse(aggLengthIterator.hasNext());
  }

  @Test
  void testOnHeapSegmentAndEncoderCtor() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).avg().count().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(rawValues);

    int length = encoder.serializationLength();
    byte[] buffer = new byte[length];
    encoder.serialize(buffer, 0, length);

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, 0, length);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, onHeapSegment);

    assertEquals(SEGMENT_TIMESTAMP, onHeapSegment.getSegmentTime());
    assertEquals(downSampler.getAggId(), onHeapSegment.getAggs());
    assertEquals(encodeInterval(interval, segmentWidth), onHeapSegment.getInterval());

    assertEquals(SEGMENT_TIMESTAMP, onHeapEncoder.getSegmentTime());
    assertEquals(segmentWidth, onHeapEncoder.getSegmentWidth());
    assertEquals(intervalCount, onHeapEncoder.getIntervalCount());
    assertEquals(interval, onHeapEncoder.getInterval());
    assertEquals(downSampler.getAggCount(), onHeapEncoder.getAggCount());
  }

  @Test
  void testSerialization() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).avg().count().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(rawValues);

    int length = encoder.serializationLength();
    byte[] buffer = new byte[length];
    encoder.serialize(buffer, 0, length);

    int index = 0;
    assertEquals(GORILLA_LOSSLESS_SECONDS, buffer[index++]);
    assertEquals(encodeInterval(interval, segmentWidth), buffer[index++]);
    assertEquals(aggregator.getId(), buffer[index++]);

    int timestampBitMapSize = intervalCount;
    int bytes = timestampBitMapSize / 8;

    for (int i = 0; i < bytes; i++) {
      // assert all bits are set
      assertEquals(-1, buffer[index++]);
    }

    downSampler.apply(rawValues);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    double[] expectedAvgs = iterator.next();
    double[] expectedCounts = iterator.next();

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, 0, length);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, onHeapSegment);

    double[] aggValues = new double[expectedAvgs.length];
    double[] countValues = new double[expectedAvgs.length];

    onHeapEncoder.readAggValues(aggValues, AverageAggregator.ID);
    onHeapEncoder.readAggValues(countValues, CountAggregator.ID);

    assertArrayEquals(expectedAvgs, aggValues);
    assertArrayEquals(expectedCounts, countValues);
  }

  @Test
  void testSerializationOfEmptySegment() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).avg().count().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);

    assertThrows(IllegalStateException.class, () -> encoder.getNumDataPoints());

    int length = encoder.serializationLength();
    byte[] buffer = new byte[length];

    assertEquals(3, length);

    encoder.serialize(buffer, 0, length);

    assertEquals(TimeSeriesEncoderType.GORILLA_LOSSLESS_SECONDS, buffer[0]);
    assertEquals(25, buffer[1]);
    assertEquals(aggregator.getId(), buffer[2]);
  }

  @Test
  void testSerializationOfWithBufferOffset() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).count().sum().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(rawValues);

    int serdeLength = encoder.serializationLength();
    byte[] buffer = new byte[serdeLength * 2];
    Arrays.fill(buffer, Byte.MAX_VALUE);

    int offset = 21;

    encoder.serialize(buffer, offset, serdeLength);

    downSampler.apply(rawValues);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    double[] expectedCounts = iterator.next();
    double[] expectedSums = iterator.next();

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, offset, serdeLength);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, onHeapSegment);

    double[] countValues = new double[expectedCounts.length];
    double[] sumValues = new double[expectedSums.length];

    onHeapEncoder.readAggValues(countValues, CountAggregator.ID);
    onHeapEncoder.readAggValues(sumValues, SumAggregator.ID);

    assertArrayEquals(expectedCounts, countValues);
    assertArrayEquals(expectedSums, sumValues);
  }

  @Test
  void testSerializationBufferTooSmall() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).sum().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(rawValues);

    int serdeLength = encoder.serializationLength();
    byte[] buffer = new byte[serdeLength - 1];
    Arrays.fill(buffer, Byte.MAX_VALUE);

    assertThrows(
        ArrayIndexOutOfBoundsException.class, () -> encoder.serialize(buffer, 0, serdeLength));
  }

  @Test
  void testSerializationPartialSegment() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    double[] partialRawValues = new double[rawValues.length / 2];
    System.arraycopy(rawValues, 0, partialRawValues, 0, partialRawValues.length);
    encoder.addDataPoints(partialRawValues);

    assertEquals(intervalCount / 2, encoder.getNumDataPoints());

    int serdeLength = encoder.serializationLength();
    byte[] buffer = new byte[serdeLength];
    encoder.serialize(buffer, 0, serdeLength);

    downSampler.apply(partialRawValues);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    double[] expectedSumOfCounts = iterator.next();

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, 0, serdeLength);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, onHeapSegment);

    double[] sumOfCounts = new double[expectedSumOfCounts.length];

    onHeapEncoder.readAggValues(sumOfCounts, SumOfSquareAggregator.ID);

    assertArrayEquals(expectedSumOfCounts, sumOfCounts);
  }

  @Test
  void testSerializationOfZeros() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).count().sum().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] zeroValues = new double[rawValues.length];
    Arrays.fill(zeroValues, 0D);

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(rawValues);

    int serdeLength = encoder.serializationLength();
    byte[] buffer = new byte[serdeLength];
    encoder.serialize(buffer, 0, serdeLength);

    downSampler.apply(rawValues);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    double[] expectedCounts = iterator.next();
    double[] expectedSums = iterator.next();

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, 0, serdeLength);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, onHeapSegment);

    double[] countValues = new double[expectedCounts.length];
    double[] sumValues = new double[expectedSums.length];

    onHeapEncoder.readAggValues(countValues, CountAggregator.ID);
    onHeapEncoder.readAggValues(sumValues, SumAggregator.ID);

    assertArrayEquals(expectedCounts, countValues);
    assertArrayEquals(expectedSums, sumValues);
  }

  @Test
  void testSerializationSingleValue() {
    Aggregator aggregator = Aggregator.newBuilder(intervalCount).max().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] singleValue = new double[1];
    singleValue[0] = rawValues[0];
    assumeFalse(Double.isNaN(singleValue[0]));

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(singleValue);

    assertEquals(1, encoder.getNumDataPoints());

    int serdeLength = encoder.serializationLength();
    byte[] buffer = new byte[serdeLength];
    encoder.serialize(buffer, 0, serdeLength);

    downSampler.apply(singleValue);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    double[] expectedAggValues = iterator.next();

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, 0, serdeLength);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, onHeapSegment);

    double[] maxValues = new double[expectedAggValues.length];
    onHeapEncoder.readAggValues(maxValues, MaxAggregator.ID);

    assertArrayEquals(expectedAggValues, maxValues);
  }

  @RepeatedTest(value = 100, name = "{displayName} - {currentRepetition}/{totalRepetitions}")
  void testSerializationOfLossLessRandomValues() {
    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).sum().count().avg().min().max().sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] randomValues = new double[segmentWidth.getWidth()];
    Arrays.fill(randomValues, Double.NaN);
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = random.nextLong() + random.nextDouble();
    }

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(randomValues);

    assertEquals(intervalCount, encoder.getNumDataPoints());

    int serdeLength = encoder.serializationLength();
    byte[] buffer = new byte[serdeLength];
    encoder.serialize(buffer, 0, serdeLength);

    downSampler.apply(randomValues);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    double[] expectedSumValues = iterator.next();
    double[] expectedCountValues = iterator.next();
    double[] expectedAvgValues = iterator.next();
    double[] expectedMinValues = iterator.next();
    double[] expectedMaxValues = iterator.next();
    double[] expectedSumOfSquaresValues = iterator.next();

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, 0, serdeLength);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, onHeapSegment);

    double[] sumValues = new double[expectedSumValues.length];
    double[] countValues = new double[expectedCountValues.length];
    double[] avgValues = new double[expectedAvgValues.length];
    double[] minValues = new double[expectedMinValues.length];
    double[] maxValues = new double[expectedMaxValues.length];
    double[] sumOfSquaresValues = new double[expectedSumOfSquaresValues.length];

    onHeapEncoder.readAggValues(sumValues, SumAggregator.ID);
    onHeapEncoder.readAggValues(countValues, CountAggregator.ID);
    onHeapEncoder.readAggValues(avgValues, AverageAggregator.ID);
    onHeapEncoder.readAggValues(minValues, MinAggregator.ID);
    onHeapEncoder.readAggValues(maxValues, MaxAggregator.ID);
    onHeapEncoder.readAggValues(sumOfSquaresValues, SumOfSquareAggregator.ID);

    assertArrayEquals(expectedSumValues, sumValues);
    assertArrayEquals(expectedCountValues, countValues);
    assertArrayEquals(expectedAvgValues, avgValues);
    assertArrayEquals(expectedMinValues, minValues);
    assertArrayEquals(expectedMaxValues, maxValues);
    assertArrayEquals(expectedSumOfSquaresValues, sumOfSquaresValues);
  }

  @RepeatedTest(value = 100, name = "{displayName} - {currentRepetition}/{totalRepetitions}")
  void testSerializationOfLossyRandomValues() {
    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).sum().count().avg().min().max().sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] randomValues = new double[segmentWidth.getWidth()];
    Arrays.fill(randomValues, Double.NaN);
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = random.nextLong() + random.nextDouble();
    }

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(true, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(randomValues);

    assertEquals(intervalCount, encoder.getNumDataPoints());

    int serdeLength = encoder.serializationLength();
    byte[] buffer = new byte[serdeLength];
    encoder.serialize(buffer, 0, serdeLength);

    downSampler.apply(randomValues);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    double[] expectedLossySumValues = toLossy(iterator.next());
    double[] expectedLossyCountValues = toLossy(iterator.next());
    double[] expectedLossyAvgValues = toLossy(iterator.next());
    double[] expectedLossyMinValues = toLossy(iterator.next());
    double[] expectedLossyMaxValues = toLossy(iterator.next());
    double[] expectedLossySumOfSquaresValues = toLossy(iterator.next());

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, 0, serdeLength);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            true, interval, segmentWidth, downSampler, onHeapSegment);

    double[] sumValues = new double[expectedLossySumValues.length];
    double[] countValues = new double[expectedLossyCountValues.length];
    double[] avgValues = new double[expectedLossyAvgValues.length];
    double[] minValues = new double[expectedLossyMinValues.length];
    double[] maxValues = new double[expectedLossyMaxValues.length];
    double[] sumOfSquaresValues = new double[expectedLossySumOfSquaresValues.length];

    onHeapEncoder.readAggValues(sumValues, SumAggregator.ID);
    onHeapEncoder.readAggValues(countValues, CountAggregator.ID);
    onHeapEncoder.readAggValues(avgValues, AverageAggregator.ID);
    onHeapEncoder.readAggValues(minValues, MinAggregator.ID);
    onHeapEncoder.readAggValues(maxValues, MaxAggregator.ID);
    onHeapEncoder.readAggValues(sumOfSquaresValues, SumOfSquareAggregator.ID);

    assertArrayEquals(expectedLossySumValues, sumValues);
    assertArrayEquals(expectedLossyCountValues, countValues);
    assertArrayEquals(expectedLossyAvgValues, avgValues);
    assertArrayEquals(expectedLossyMinValues, minValues);
    assertArrayEquals(expectedLossyMaxValues, maxValues);
    assertArrayEquals(expectedLossySumOfSquaresValues, sumOfSquaresValues);
  }

  @RepeatedTest(value = 10, name = "{displayName} - {currentRepetition}/{totalRepetitions}")
  void testSerializationOfSparseData() {
    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).sum().count().avg().min().max().sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] sparseValues = new double[segmentWidth.getWidth()];
    Arrays.fill(sparseValues, Double.NaN);
    sparseValues[39] = random.nextLong() + random.nextDouble();

    sparseValues[239] = random.nextLong() + random.nextDouble();

    sparseValues[240] = random.nextLong() + random.nextDouble();
    sparseValues[242] = random.nextLong() + random.nextDouble();
    sparseValues[243] = random.nextLong() + random.nextDouble();
    sparseValues[244] = random.nextLong() + random.nextDouble();

    sparseValues[7198] = random.nextLong() + random.nextDouble();

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(
            false, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(sparseValues);

    assertEquals(4, encoder.getNumDataPoints());

    int serdeLength = encoder.serializationLength();
    byte[] buffer = new byte[serdeLength];
    encoder.serialize(buffer, 0, serdeLength);

    downSampler.apply(sparseValues);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    double[] expectedSumValues = iterator.next();
    double[] expectedCountValues = iterator.next();
    double[] expectedAvgValues = iterator.next();
    double[] expectedMinValues = iterator.next();
    double[] expectedMaxValues = iterator.next();
    double[] expectedSumOfSquaresValues = iterator.next();

    assumeSparseData(expectedSumValues, sparseValues);
    assumeSparseData(expectedCountValues, sparseValues);
    assumeSparseData(expectedAvgValues, sparseValues);
    assumeSparseData(expectedMinValues, sparseValues);
    assumeSparseData(expectedMaxValues, sparseValues);
    assumeSparseData(expectedSumOfSquaresValues, sparseValues);

    OnHeapGorillaDownSampledSegment onHeapSegment =
        new OnHeapGorillaDownSampledSegment(SEGMENT_TIMESTAMP, buffer, 0, serdeLength);
    GorillaDownSampledTimeSeriesEncoder onHeapEncoder =
        new GorillaDownSampledTimeSeriesEncoder(
            true, interval, segmentWidth, downSampler, onHeapSegment);

    double[] sumValues = new double[expectedSumValues.length];
    double[] countValues = new double[expectedCountValues.length];
    double[] avgValues = new double[expectedAvgValues.length];
    double[] minValues = new double[expectedMinValues.length];
    double[] maxValues = new double[expectedMaxValues.length];
    double[] sumOfSquaresValues = new double[expectedSumOfSquaresValues.length];

    onHeapEncoder.readAggValues(sumValues, SumAggregator.ID);
    onHeapEncoder.readAggValues(countValues, CountAggregator.ID);
    onHeapEncoder.readAggValues(avgValues, AverageAggregator.ID);
    onHeapEncoder.readAggValues(minValues, MinAggregator.ID);
    onHeapEncoder.readAggValues(maxValues, MaxAggregator.ID);
    onHeapEncoder.readAggValues(sumOfSquaresValues, SumOfSquareAggregator.ID);

    assertArrayEquals(expectedSumValues, sumValues);
    assertArrayEquals(expectedCountValues, countValues);
    assertArrayEquals(expectedAvgValues, avgValues);
    assertArrayEquals(expectedMinValues, minValues);
    assertArrayEquals(expectedMaxValues, maxValues);
    assertArrayEquals(expectedSumOfSquaresValues, sumOfSquaresValues);
  }

  @Test
  void freeSegment() {
    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).sum().count().avg().min().max().sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] randomValues = new double[segmentWidth.getWidth()];
    Arrays.fill(randomValues, Double.NaN);
    for (int i = 0; i < randomValues.length; i++) {
      randomValues[i] = random.nextLong() + random.nextDouble();
    }

    encoder =
        new GorillaDownSampledTimeSeriesEncoder(true, interval, segmentWidth, downSampler, segment);
    encoder.createSegment(SEGMENT_TIMESTAMP);
    encoder.addDataPoints(randomValues);

    encoder.freeSegment();
  }

  private void assumeSparseData(double[] aggValues, double[] rawValues) {
    assumeTrue(aggValues.length == rawValues.length / intervalWidth);
    for (int i = 0; i < rawValues.length; i++) {
      if (!Double.isNaN(rawValues[i])) {
        assumeFalse(Double.isNaN(aggValues[i / intervalWidth]));
      }
    }
  }

  private static double[] toLossy(double[] values) {
    double[] lossyValues = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      lossyValues[i] = Double.longBitsToDouble(Double.doubleToRawLongBits(values[i]) & LOSS_MASK);
    }
    return lossyValues;
  }
}
