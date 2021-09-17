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
import io.ultrabrew.metrics.MetricRegistry;
import net.opentsdb.hashing.HashFunction;
import net.opentsdb.stats.StatsCollector;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EphemeralStorage extends BaseStorage {

  /** Sample value: 0 or 2 or 12 */
  private final int segmentWallClockHour;

  private LocalDateTime segmentDateTime;

  private EphemeralStorageContext storageContext;

  private volatile boolean closed;

  public EphemeralStorage(
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
        new EphemeralStorageContext(shardConfig),
        scheduledExecutorService);

    this.storageContext = (EphemeralStorageContext) context;
    this.segmentWallClockHour = shardConfig.getSegmentWallClockHour();
    openSegment();

    Timer timer = new Timer(true);
    scheduleSegmentOpen(timer);
    scheduleSegmentClose(timer);

    LOGGER.info(
        "{} storage started for segment wall clock hour: {}, segment time: {}",
        StorageMode.EPHEMERAL.name(),
        segmentWallClockHour,
        storageContext.segmentTime);
  }

  private void scheduleSegmentClose(final Timer timer) {
    LocalDateTime closingTime =
        segmentDateTime
            .plusHours(context.getSegmentSizeHour())
            .plusMinutes(15); // waits 15 minutes to accommodate any delayed data.
    LocalDateTime now = LocalDateTime.now();
    if (closingTime.isBefore(now)) {
      closeSegment();
      closingTime = closingTime.plusDays(1);
    }
    long delay = now.until(closingTime, ChronoUnit.MILLIS);
    long period = TimeUnit.HOURS.toMillis(24); // every 24 hours

    timer.scheduleAtFixedRate(
        new TimerTask() {
          @Override
          public void run() {
            closeSegment();
          }
        },
        delay,
        period);

    LOGGER.info(
        "Segment scheduled for closing closingTime: {}, delay: {}, period: {}",
        closingTime,
        delay,
        period);
  }

  private void scheduleSegmentOpen(final Timer timer) {
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime openingTime = segmentDateTime.minusSeconds(30);
    if (openingTime.isBefore(now)) {
      openingTime = openingTime.plusDays(1);
    }
    long delay = now.until(openingTime, ChronoUnit.MILLIS);
    long period = TimeUnit.HOURS.toMillis(24); // every 24 hours
    timer.scheduleAtFixedRate(
        new TimerTask() {
          @Override
          public void run() {
            openSegment();
          }
        },
        delay,
        period);

    LOGGER.info(
        "Segment scheduled for opening openingTime: {}, delay: {}, period: {}",
        openingTime,
        delay,
        period);
  }

  private void openSegment() {

    this.closed = false;
    LocalDateTime now = LocalDateTime.now();
    LocalDate date = now.toLocalDate();
    LocalTime time = LocalTime.of(segmentWallClockHour, 0, 0);
    this.segmentDateTime = LocalDateTime.of(date, time);

    while (segmentDateTime.plusHours(context.getSegmentSizeHour()).plusMinutes(15).isBefore(now)) {
      this.segmentDateTime = segmentDateTime.plusDays(1);
    }
    this.storageContext.segmentTime =
        (int) segmentDateTime.atZone(ZoneId.systemDefault()).toEpochSecond();

    LOGGER.info("Segment opened for segmentTime: {}", segmentDateTime);
  }

  private void closeSegment() {
    this.closed = true;
    LOGGER.info("Segment closed for segmentTime: {}", segmentDateTime);

    if (flusher != null) {
      flushShards();
    }
  }

  private void flushShards() {
    LOGGER.info("Flushing shards");
    for (int i = 0; i < shards.length; i++) {
      shards[i].flush();
    }
  }

  @Override
  protected boolean isDelayed(final int dataSegmentTime, final int currentTimeHour) {
    return dataSegmentTime < storageContext.segmentTime || closed;
  }

  @Override
  protected boolean isEarly(final int dataSegmentTime, final int currentTimeHour) {
    return dataSegmentTime > storageContext.segmentTime || closed;
  }

  static class EphemeralStorageContext extends TimeseriesStorageContext {

    private static final int[] EMPTY_SEGMENT_TIMES = new int[0];

    /** Epoch second for the wall clock hour {@link #segmentWallClockHour} */
    private volatile int segmentTime;

    public EphemeralStorageContext(final ShardConfig shardConfig) {
      // Stores only 1 segment. Hence retentionHour == segmentSizeHour
      super(shardConfig.segmentSizeHour, shardConfig.segmentSizeHour, StorageMode.EPHEMERAL);
    }

    @Override
    public long getSegmentTimes(int queryStartTime, int queryEndTime) {
      if (queryStartTime <= (segmentTime + secondsInASegment) && segmentTime < queryEndTime) {
        // Encode segment time and segment count into a long.
        // Upper 32 bits represent the segment time and lower 32 bits represent the segment count.
        return ((long) segmentTime) << 32 | ((long) 1);
      }
      return 0;
    }

  }
}
