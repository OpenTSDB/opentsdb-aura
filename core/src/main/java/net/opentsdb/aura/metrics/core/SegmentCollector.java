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

package net.opentsdb.aura.metrics.core;

import io.ultrabrew.metrics.Gauge;
import io.ultrabrew.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class SegmentCollector implements LazyStatsCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCollector.class);

  private final GarbageQueue garbageQueue;
  private final int collectionDelayMinutes;
  private final Segment segmentHandle;
  private final LazyStatsCollector segmentStatsCollector;
  private Gauge garbageQueueLengthGauge;
  private String[] tags;

  public SegmentCollector(
      final int garbageQSize,
      final int collectionDelayMinutes,
      final Segment segmentHandle,
      final MetricRegistry metricRegistry) {
    this.collectionDelayMinutes = collectionDelayMinutes;
    this.garbageQueue = new GarbageQueue("GarbageQueue", garbageQSize);
    this.segmentHandle = segmentHandle;
    this.segmentStatsCollector = (LazyStatsCollector) segmentHandle;
    this.garbageQueueLengthGauge = metricRegistry.gauge("garbage.queue.length");
  }

  public void collect(final long segmentAddress) {
    garbageQueue.add(segmentAddress, System.currentTimeMillis());
  }

  public void freeSegments() {
    int count = 0;
    long lastMS = 0;

    while (garbageQueue.size() > 0) {
      final long now = System.currentTimeMillis();
      long time = garbageQueue.peekTime();
      final long timeElapsedMillis = now - time;
      final long timeElapsedMinutes = TimeUnit.MILLISECONDS.toMinutes(timeElapsedMillis);
      if (timeElapsedMinutes >= collectionDelayMinutes) {
        segmentHandle.openSegment(garbageQueue.peekAddress());
        segmentHandle.free();
        garbageQueue.remove();
        lastMS = time;
        count++;
      } else {
        break;
      }
    }

    final long lastEpoch = lastMS / 1000;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Segments collected for count: {} epoch: {}", count, lastEpoch);
    }
    collectMetrics();
  }

  @Override
  public void collectMetrics() {
    garbageQueueLengthGauge.set(garbageQueue.size(), tags);
    segmentStatsCollector.collectMetrics();
  }

  @Override
  public void setTags(final String[] tags) {
    this.tags = tags;
  }
}
