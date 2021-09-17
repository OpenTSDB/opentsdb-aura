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

import net.opentsdb.aura.metrics.meta.MetaDataStoreFactory;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.stats.StatsCollector;

import java.util.concurrent.ScheduledExecutorService;

public class LongRunningStorage extends BaseStorage {

  public LongRunningStorage(
          final MemoryInfoReader memoryInfoReader,
          final ShardConfig shardConfig,
          final StatsCollector stats,
          final HashFunction hashFunction,
          final TimeSeriesEncoderFactory encoderFactory,
          final MetaDataStoreFactory metaDataStoreFactory,
          final Flusher flusher,
          final ScheduledExecutorService scheduledExecutorService) {
    super(
        memoryInfoReader,
        shardConfig,
        stats,
        hashFunction,
        encoderFactory,
        metaDataStoreFactory,
        flusher,
        new LongRunningStorageContext(shardConfig),
        scheduledExecutorService);
  }

  @Override
  protected boolean isDelayed(final int segmentTime, final int currentTimeHour) {
    return (currentTimeHour - segmentTime) / Util.SECONDS_IN_AN_HOUR > context.getRetentionHour();
  }

  @Override
  protected boolean isEarly(final int segmentTime, final int currentTimeHour) {
    return segmentTime > currentTimeHour;
  }

  public static class LongRunningStorageContext extends TimeseriesStorageContext {

    public LongRunningStorageContext(final ShardConfig shardConfig) {
      super(shardConfig.retentionHour, shardConfig.segmentSizeHour, StorageMode.LONG_RUNNING);
    }

    @Override
    public long getSegmentTimes(final int queryStartTime, final int queryEndTime) {
      int startSegmentHour = 0;
      int segmentCount = 0;
      if (queryStartTime <= queryEndTime) {

        startSegmentHour = getSegmentTime(queryStartTime);
        int endSegmentHour = getSegmentTime(queryEndTime);
        int currentSegmentHour = getSegmentTime((int) (System.currentTimeMillis() / 1000));

        if ((currentSegmentHour - endSegmentHour) >= secondsInATimeSeries
            || startSegmentHour > currentSegmentHour) {
          // Query outside of the storage range
          return 0;
        }
        if ((currentSegmentHour - startSegmentHour) >= secondsInATimeSeries) {
          // if start time is before the first segment, snap it to the first segment time
          startSegmentHour = currentSegmentHour - secondsInATimeSeries + secondsInASegment;
        }
        if (endSegmentHour > currentSegmentHour) {
          // if end time is after last segment, snap it to the last segment time
          endSegmentHour = currentSegmentHour;
        }
        segmentCount = (endSegmentHour - startSegmentHour) / secondsInASegment + 1;
      }
      // Encode segment time and segment count into a long.
      // Upper 32 bits represent the segment time and lower 32 bits represent the segment count.
      return ((long) startSegmentHour) << 32 | ((long) segmentCount);
    }

  }
}
