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

import net.opentsdb.aura.metrics.core.TimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.downsample.Aggregator;
import net.opentsdb.aura.metrics.core.downsample.DownSampler;
import net.opentsdb.aura.metrics.core.downsample.Interval;
import net.opentsdb.aura.metrics.core.downsample.SegmentWidth;

import java.util.List;

public class GorillaTimeSeriesDownSamplingEncoderFactory
    implements TimeSeriesEncoderFactory<GorillaDownSampledTimeSeriesEncoder> {

  private boolean lossy;
  private Interval interval;
  private SegmentWidth segmentWidth;
  private short intervalCount;
  private List<String> aggs;
  private GorillaSegmentFactory<OffHeapGorillaDownSampledSegment> segmentFactory;

  public GorillaTimeSeriesDownSamplingEncoderFactory(
      final boolean lossy,
      final Interval interval,
      final SegmentWidth segmentWidth,
      final List<String> aggs,
      final GorillaSegmentFactory segmentFactory) {
    this.lossy = lossy;
    this.interval = interval;
    this.segmentWidth = segmentWidth;
    this.segmentFactory = segmentFactory;
    if (aggs.isEmpty()) {
      throw new IllegalArgumentException("No down sampling agg found");
    }
    this.aggs = aggs;
    this.intervalCount = interval.getCount(segmentWidth);
  }

  @Override
  public GorillaDownSampledTimeSeriesEncoder create() {

    OffHeapGorillaDownSampledSegment segmentHandle = segmentFactory.create();
    Aggregator.AggregatorBuilder aggBuilder = Aggregator.newBuilder(intervalCount);
    aggs.stream().forEach(agg -> aggBuilder.forType(agg));
    DownSampler downSampler =
        new DownSampler(interval.getSeconds(), intervalCount, aggBuilder.build());
    return new GorillaDownSampledTimeSeriesEncoder(
        lossy, interval, segmentWidth, downSampler, segmentHandle);
  }
}
