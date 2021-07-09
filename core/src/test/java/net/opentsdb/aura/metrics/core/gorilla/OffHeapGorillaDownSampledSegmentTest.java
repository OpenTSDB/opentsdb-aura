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

import io.ultrabrew.metrics.MetricRegistry;
import mockit.Mocked;
import net.opentsdb.aura.metrics.core.downsample.Aggregator;
import net.opentsdb.aura.metrics.core.downsample.Interval;
import net.opentsdb.aura.metrics.core.downsample.DownSampler;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffHeapGorillaDownSampledSegmentTest {

  private static final int SEGMENT_TIMESTAMP = 1620237600;

  @Test
  void encodeTimesAndValues(@Mocked MetricRegistry registry) {

//    Aggregator aggregator = Aggregator.newBuilder().avg().build();
//    Interval downSampleInterval = Interval._30_SEC;
//    byte intervalCount = (byte) 240;
//    double[] values = new double[240];
//    Arrays.fill(values, Double.NaN);
//    values[0] = 10.15;
//    values[10] = 25.25;
//
//    OffHeapDownSampledGorillaSegment segment = new OffHeapDownSampledGorillaSegment(256, registry);
//    segment.create(SEGMENT_TIMESTAMP);
//    segment.setInterval(downSampleInterval.getId());
////    segment.setIntervalCount(intervalCount);
//    segment.setAggs(aggregator.getId());
//
//    // encode timestamp bits.
//    long bitMap = 0;
//    int offset = 0;
//    final int lastIndex = values.length - 1;
//    for (int i = 0; i < values.length; i++) {
//      offset++;
//      if (!Double.isNaN(values[i])) {
//        bitMap = bitMap | (1l << (64 - offset));
//      }
//
//      if (offset == 64 || i == lastIndex) {
//        System.out.println(bitMap);
//        segment.write(bitMap, offset);
//        bitMap = 0;
//        offset = 0;
//      }
//    }
//
//    // encode aggs
////    dataPoints = 0;
//    for (int i = 0; i <values.length; i++) {
////      appendValue(values[i]);
//    }
//
//
//
//    DownSampler downSampler =
//        new DownSampler(downSampleInterval.getWidth(), intervalCount, aggregator);
//    downSampler.apply(values);
//
////    segment.resetCursor();
//
//    assertEquals(SEGMENT_TIMESTAMP, segment.getSegmentTime());
//    assertEquals(downSampleInterval.getId(), segment.getInterval());
////    assertEquals(intervalCount, segment.getIntervalCount());
  }

}
