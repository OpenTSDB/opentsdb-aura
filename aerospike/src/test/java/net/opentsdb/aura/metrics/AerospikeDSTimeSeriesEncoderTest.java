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

package net.opentsdb.aura.metrics;

import net.opentsdb.aura.metrics.core.downsample.AggregationLengthIterator;
import net.opentsdb.aura.metrics.core.downsample.Aggregator;
import net.opentsdb.aura.metrics.core.downsample.AggregatorIterator;
import net.opentsdb.aura.metrics.core.downsample.DownSampler;
import net.opentsdb.aura.metrics.core.downsample.Interval;
import net.opentsdb.aura.metrics.core.downsample.SegmentWidth;
import net.opentsdb.aura.metrics.core.gorilla.GorillaDownSampledTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.OffHeapGorillaDownSampledSegment;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AerospikeDSTimeSeriesEncoderTest {

  private static final long LOSS_MASK = 0xFFFFFFFFFF000000l;
  private static final int SEGMENT_TIMESTAMP = 1611288000;

  private static Interval interval = Interval._30_SEC;
  private static SegmentWidth segmentWidth = SegmentWidth._2_HR;
  private static int intervalWidth = interval.getSeconds();
  private static short intervalCount = interval.getCount(segmentWidth);
  private static double[] rawValues = new double[segmentWidth.getSeconds()];
  private static Random random = new Random(System.currentTimeMillis());

  private OffHeapGorillaDownSampledSegment segment = new OffHeapGorillaDownSampledSegment(256);

  private GorillaDownSampledTimeSeriesEncoder encoder;

  @RepeatedTest(value = 100, name = "{displayName} - {currentRepetition}/{totalRepetitions}")
  void serializeAggValuesSeparately() {
    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).sum().count().avg().min().max().sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] randomValues = new double[segmentWidth.getSeconds()];
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

    downSampler.apply(randomValues);
    AggregatorIterator<double[]> iterator = downSampler.iterator();
    Map<Byte, double[]> expectedValues = new HashMap<>();
    while (iterator.hasNext()) {
      double[] values = iterator.next();
      expectedValues.put(iterator.aggID(), values);
    }

    // header bytes
    int headerBytes = (int) (3 + Math.ceil(encoder.getIntervalCount() / Byte.SIZE));
    byte[] header = new byte[headerBytes];

    encoder.serializeHeader(header, 0);

    AggregationLengthIterator aggIterator = encoder.aggIterator();
    while (aggIterator.hasNext()) {
      aggIterator.next();
      int aggBytes = aggIterator.aggLengthInBytes();
      // one extra byte to accommodate if agg is not starting from the byte boundary.
      byte[] agg = new byte[aggBytes + 1];

      aggIterator.serialize(agg, 0);

      OnHeapAerospikeSegment aerospikeSegment =
          new OnHeapAerospikeSegment(
              SEGMENT_TIMESTAMP, header, new byte[] {aggIterator.aggID()}, new byte[][] {agg});
      AerospikeDSTimeSeriesEncoder aerospikeEncoder =
          new AerospikeDSTimeSeriesEncoder(aerospikeSegment);

      assertEquals(intervalCount, aerospikeEncoder.getNumDataPoints());

      double[] values = new double[intervalCount];

      int numPoints = aerospikeEncoder.readAggValues(values, aggIterator.aggID());
      assertEquals(intervalCount, numPoints);
      assertArrayEquals(expectedValues.get(aggIterator.aggID()), values);
    }
  }

  @Test
  void testSerializeSparseDataSeparately() {
    Aggregator aggregator =
        Aggregator.newBuilder(intervalCount).sum().count().avg().min().max().sumOfSquares().build();
    DownSampler downSampler = new DownSampler(intervalWidth, intervalCount, aggregator);

    double[] sparseValues = new double[segmentWidth.getSeconds()];
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

    Map<Byte, double[]> expectedValues = new HashMap<>();
    while (iterator.hasNext()) {
      double[] values = iterator.next();
      expectedValues.put(iterator.aggID(), values);
    }

    // header bytes
    int headerBytes = (int) (3 + Math.ceil(encoder.getIntervalCount() / Byte.SIZE));
    byte[] header = new byte[headerBytes];

    encoder.serializeHeader(header, 0);

    AggregationLengthIterator aggIterator = encoder.aggIterator();
    while (aggIterator.hasNext()) {
      aggIterator.next();
      int aggBytes = aggIterator.aggLengthInBytes();
      // one extra byte to accommodate if agg is not starting from the byte boundary.
      byte[] agg = new byte[aggBytes + 1];

      aggIterator.serialize(agg, 0);

      OnHeapAerospikeSegment aerospikeSegment =
          new OnHeapAerospikeSegment(
              SEGMENT_TIMESTAMP, header, new byte[] {aggIterator.aggID()}, new byte[][] {agg});
      AerospikeDSTimeSeriesEncoder aerospikeEncoder =
          new AerospikeDSTimeSeriesEncoder(aerospikeSegment);

      assertEquals(4, aerospikeEncoder.getNumDataPoints());

      double[] values = new double[intervalCount];

      int numPoints = aerospikeEncoder.readAggValues(values, aggIterator.aggID());
      assertEquals(4, numPoints);
      assertArrayEquals(expectedValues.get(aggIterator.aggID()), values);
    }
  }
}
