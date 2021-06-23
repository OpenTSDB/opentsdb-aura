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

import net.opentsdb.aura.metrics.meta.MetaDataStore;
import net.opentsdb.aura.metrics.meta.MetaDataStoreFactory;
import net.opentsdb.aura.metrics.meta.MetricQuery;
import io.ultrabrew.metrics.Counter;
import io.ultrabrew.metrics.MetricRegistry;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.hashing.HashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class BaseStorage implements TimeSeriesStorageIf {

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  private final ShardConfig shardConfig;

  protected final TimeSeriesShardIF[] shards;
  private final int shardCount;
  private final int[] shardIds;
  protected final Flusher flusher;

  private final Counter earlyTimeSeriesCounter;
  private final Counter delayedTimeSeriesCounter;
  protected TimeseriesStorageContext context;

  public BaseStorage(
      final MemoryInfoReader memoryInfoReader,
      final ShardConfig shardConfig,
      final MetricRegistry metricRegistry,
      final HashFunction hashFunction,
      final TimeSeriesEncoderFactory encoderFactory,
      final MetaDataStoreFactory metaDataStoreFactory,
      final Flusher flusher,
      final TimeseriesStorageContext context,
      final ScheduledExecutorService scheduledExecutorService) {

    this.shardConfig = shardConfig;
    this.shardIds = shardConfig.shardIds;
    this.flusher = flusher;
    this.context = context;
    this.shardCount = shardIds.length;
    this.shards = new TimeSeriesShardIF[shardCount];

    LocalDateTime purgeDateTime = Util.getPurgeTime();
    for (int i = 0; i < shardCount; i++) {
      int shardId = shardIds[i];
      TimeSeriesEncoder encoder = encoderFactory.create();
      MetaDataStore metaDataStore = metaDataStoreFactory.create();
      TimeSeriesShard shard =
          new TimeSeriesShard(
              shardId,
              shardConfig,
              context,
              encoder,
              metaDataStore,
              memoryInfoReader,
              metricRegistry,
              purgeDateTime,
              hashFunction,
              flusher,
              scheduledExecutorService);
      // TODO: till the encapsulation of DocStore is fixed.
      ((ShardAware) metaDataStore).setShard(shard);
      shards[i] = shard;
    }

    this.earlyTimeSeriesCounter = metricRegistry.counter("timeseries.early.count");
    this.delayedTimeSeriesCounter = metricRegistry.counter("timeseries.delay.count");
  }

  protected abstract boolean isDelayed(int segmentTime, int currentTimeHour);

  protected abstract boolean isEarly(int dataTime, int currentTimeHour);

  @Override
  public void addEvent(final LowLevelMetricData.HashedLowLevelMetricData event) {

    int eventTimestamp = (int) event.timestamp().epoch();
    int segmentTime = context.getSegmentTime(eventTimestamp);
    int currentTimeHour = Util.getCurrentWallClockHour();

    if (isDelayed(segmentTime, currentTimeHour)) { // older than retention period. drop it.
      delayedTimeSeriesCounter.inc();
      try {
        event.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close event", e);
      }
      return;
    }

    if (isEarly(segmentTime, currentTimeHour)) { // too early, drop it.
      earlyTimeSeriesCounter.inc();
      try {
        event.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close event", e);
      }
      return;
    }

    int shardIndex = (int) (Math.abs(event.tagsSetHash() % shardCount));
    if (shardIndex < 0) {
      shardIndex = 0;
    }

    TimeSeriesShardIF shard = shards[shardIndex];

    try {
      ((ShardAware) event).setShard(shard);
      shard.submit((Runnable) event);
    } catch (InterruptedException e) {
      LOGGER.error("Metrics writer thread interrupted", e);
      try {
        event.close();
      } catch (IOException ex) {
        LOGGER.warn("Failed to close event", ex);
      }
    }
  }

  @Override
  public long read(
      final MetricQuery query,
      final int startTime,
      final int endTime,
      final ResultConsumer consumer) {
    final long segmentTimes = context.getSegmentTimes(startTime, endTime);
    if (segmentTimes == 0) {
      return 0;
    }
    final int firstSegmentTime = segmentTimes > 0 ? context.getFirstSegmentTime(segmentTimes) : 0;
    final int segmentCount = segmentTimes > 0 ? context.getSegmentCount(segmentTimes) : 0;
    final CountDownLatch doneSignal = new CountDownLatch(shardCount);
    try {
      for (int i = 0; i < shardCount; i++) {
        int shardIndex = i;
        TimeSeriesShardIF shard = shards[shardIndex];
        shard.submit(
            () -> {
              try {
                long resultAddress = shard.query(query, firstSegmentTime, segmentCount);
                consumer.consume(shardIndex, resultAddress);
              } catch (Throwable t) {
                LOGGER.error("Error reading shard: " + shardIds[shardIndex], t);
                consumer.consume(t);
              }
              doneSignal.countDown();
            });
      }
      doneSignal.await();
    } catch (InterruptedException e) {
      LOGGER.error("Shard read interrupted", e);
    }
    return segmentTimes;
  }

  @Override
  public void readLastValue(final MetricQuery query, final ResultConsumer consumer) {
    final CountDownLatch doneSignal = new CountDownLatch(shardCount);
    try {
      for (int i = 0; i < shardCount; i++) {
        int shardIndex = i;
        TimeSeriesShardIF shard = shards[shardIndex];
        shard.submit(
            () -> {
              try {
                long resultAddress = shard.query(query);
                consumer.consume(shardIndex, resultAddress);
              } catch (Throwable t) {
                LOGGER.error("Error reading shard: " + shardIds[shardIndex], t);
                consumer.consume(t);
              }
              doneSignal.countDown();
            });
      }
      doneSignal.await();
    } catch (InterruptedException e) {
      LOGGER.error("Shard read interrupted", e);
    }
  }

  @Override
  public int numShards() {
    return shardCount;
  }

  @Override
  public int getSegmentHour(final int timeInSeconds) {
    return timeInSeconds
        - (timeInSeconds % (int) TimeUnit.HOURS.toSeconds(shardConfig.segmentSizeHour));
  }

  @Override
  public int retentionInHours() {
    return context.getRetentionHour();
  }

  @Override
  public int getShardId(final long hash) {
    int shardId = (int) (Math.abs(hash) % shardCount);
    if (shardId < 0) {
      shardId = 0;
    }
    return shardId;
  }

  @Override
  public TimeSeriesShardIF getShard(final int shardId) {
    return shards[shardId];
  }

  @Override
  public int secondsInSegment() {
    return context.getSecondsInASegment();
  }

  @Override
  public long getDataPointCount() {
    long count = 0;
    for (int i = 0; i < shardCount; i++) {
      count += shards[i].getDataPointCount();
    }
    return count;
  }

  @Override
  public long getDataDropCount() {
    long count = 0;
    for (int i = 0; i < shardCount; i++) {
      count += shards[i].getDataDropCount();
    }
    return count;
  }

  @Override
  public TimeseriesStorageContext getContext() {
    return context;
  }
}
