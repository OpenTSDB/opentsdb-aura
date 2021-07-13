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
import io.ultrabrew.metrics.Timer;
import net.opentsdb.aura.metrics.core.downsample.DownSamplingTimeSeriesEncoder;
import net.opentsdb.collections.LongLongHashTable;
import net.opentsdb.collections.LongLongIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * This is a flusher that will take a shard, the clone of a shard's hash table and a set of one or
 * more stores to write to and flushes each segment found for the base time.
 */
public class TSFlusherImp implements TSFlusher {
  private static final Logger LOGGER = LoggerFactory.getLogger(TSFlusherImp.class);

  private final MetricRegistry registry;
  private final List<LongTermStorage> stores;
  private final ThreadLocal<int[]> tlTimestamps;
  private final ThreadLocal<double[]> tlValues;
  private ThreadLocal<BasicTimeSeriesEncoder> encoders;
  private ThreadLocal<DownSamplingTimeSeriesEncoder> dsEncoders;
  private final FlushInfo[] flushInfos;
  private final TimeSeriesRecordFactory recordFactory;
  private final TimeSeriesEncoderFactory<BasicTimeSeriesEncoder> encoderFactory;
  private final TimeSeriesEncoderFactory<DownSamplingTimeSeriesEncoder> dsEncoderFactory;
  private final String[] tagSet;
  private final long frequency;
  private final int threadsPerShard;
  private volatile int lastStart;

  public TSFlusherImp(
      final TimeSeriesRecordFactory recordFactory,
      final TimeSeriesEncoderFactory<BasicTimeSeriesEncoder> encoderFactory,
      final TimeSeriesEncoderFactory<DownSamplingTimeSeriesEncoder> dsEncoderFactory,
      final ShardConfig shardConfig,
      final List<LongTermStorage> stores,
      final ScheduledExecutorService scheduledExecutorService,
      final MetricRegistry registry,
      final String[] tagSet,
      final long frequency,
      final int threadsPerShard) {
    this.stores = stores;
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(shardConfig.segmentSizeHour);
    tlTimestamps = ThreadLocal.withInitial(() -> new int[secondsInASegment]);
    tlValues = ThreadLocal.withInitial(() -> new double[secondsInASegment]);
    flushInfos = new FlushInfo[shardConfig.shardCount];
    this.recordFactory = recordFactory;
    this.encoderFactory = encoderFactory;
    this.dsEncoderFactory = dsEncoderFactory;
    if (dsEncoderFactory == null) {
      encoders = ThreadLocal.withInitial(() -> encoderFactory.create());
    } else {
      dsEncoders = ThreadLocal.withInitial(() -> dsEncoderFactory.create());
    }
    this.registry = registry;
    this.tagSet = tagSet;
    this.frequency = frequency;
    this.threadsPerShard = threadsPerShard;
    scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            // Little locking as we don't really care about the races.
            StringBuilder buf = new StringBuilder();
            int nulls = 0;
            for (int i = 0; i < flushInfos.length; i++) {
              if (i > 0) {
                buf.append(", ");
              }
              FlushInfo fi = flushInfos[i];
              if (fi == null) {
                nulls++;
                buf.append("[" + i + "] not running. ");
                continue;
              }

              // TODO kill super long running flushes as something is wrong.
              synchronized (flushInfos) {
                int ts = (int) fi.start / 1_000_000_000;
                if (ts + (int) ((frequency / 1000) * 2) < System.currentTimeMillis() / 1000) {
                  LOGGER.warn(
                      "Flush for shard {}@{} has been running for {} so cancelling",
                      i,
                      fi.baseTimestamp,
                      ((int) (System.currentTimeMillis() / 1000) - ts));
                  // fi.cancel();
                }
              }

              int flushed = fi.flushed;
              int ooo = fi.ooo;
              int failed = fi.failures;
              int noSegment = fi.noSegment;

              for (int x = 0; x < fi.jobs.length; x++) {
                FlushInfo.Job j = fi.jobs[x];
                if (j == null) {
                  continue;
                }
                flushed += j.flushed;
                ooo += j.ooo;
                failed += j.failures;
                noSegment += j.noSegment;
              }
              buf.append("[" + i + "] F: ")
                  .append(flushed)
                  .append(" O: ")
                  .append(ooo)
                  .append(" E: ")
                  .append(failed)
                  .append(" N: ")
                  .append(noSegment);
            }

            if (nulls == flushInfos.length) {
              buf.setLength(0);
              buf.append("No flush jobs running.");
            }
            LOGGER.info("[FLUSHER] {}", buf.toString());
          } catch (Throwable throwable) {
            LOGGER.error("Error reporting stats on flusher", throwable);
          }
        },
        MINUTES.toMillis(1),
        MINUTES.toMillis(1),
        MILLISECONDS);
  }

  @Override
  public long frequency() {
    return frequency;
  }

  @Override
  public FlushStatus flushShard(
      final int shardId, final LongLongHashTable timeSeriesTable, final int flushTimestamp) {
    final int ts = (int) System.currentTimeMillis() / 1000;
    FlushStatus flushStatus;
    synchronized (flushInfos) {
      if (flushInfos[shardId] == null) {
        AtomicBoolean inProgress = new AtomicBoolean(true);
        flushInfos[shardId] = new FlushInfo(shardId, timeSeriesTable, flushTimestamp, inProgress);
        flushStatus = new TSFlusherStatus(inProgress);
        if (ts > lastStart) {
          lastStart = ts;
        }
      } else {
        flushStatus = new TSFlusherStatus(new AtomicBoolean(false));
        LOGGER.error(
            "Failed to start flush for shard {}@{} as a flush is still ongoing.",
            shardId,
            flushTimestamp);
      }
    }
    return flushStatus;
  }

  /**
   * Instantiated for each flush at each timestamp. This holds the jobs (a single thread) so we can
   * run multiple threads if we need to.
   */
  class FlushInfo {
    private final String[] tagSet;
    private final Timer totalTimer;
    private final long start;
    private final int shardId;
    private final LongLongIterator iterator;
    private final LongLongHashTable timeSeriesTable;
    private final int baseTimestamp;
    private final Job[] jobs;
    private final Gauge flushedGuage;
    private final Gauge oooGuage;
    private final Gauge failureGuage;
    private final Gauge emptyGuage;
    private final AtomicBoolean inProgress;
    private int flushed;
    private int ooo;
    private int failures;
    private int noSegment;

    FlushInfo(
        final int shardId,
        final LongLongHashTable timeSeriesTable,
        final int baseTimestamp,
        final AtomicBoolean inProgress) {
      this.inProgress = inProgress;
      totalTimer = registry.timer("storage.flusher.timeTaken");
      start = totalTimer.start();
      tagSet = Arrays.copyOf(TSFlusherImp.this.tagSet, TSFlusherImp.this.tagSet.length);
      tagSet[tagSet.length - 1] = Integer.toString(shardId);
      this.shardId = shardId;
      this.timeSeriesTable = timeSeriesTable;
      iterator = timeSeriesTable.iterator();
      this.baseTimestamp = baseTimestamp;
      jobs = new Job[threadsPerShard];
      flushedGuage = registry.gauge("storage.flusher.segmentsFlushed");
      oooGuage = registry.gauge("storage.flusher.segmentsOutOfOrder");
      failureGuage = registry.gauge("storage.flusher.segmentFlushFailures");
      emptyGuage = registry.gauge("storage.flusher.segmentEmpty");
      for (int i = 0; i < jobs.length; i++) {
        jobs[i] = new Job(i);
        jobs[i].start();
      }

      LOGGER.info("Starting {} flush jobs for shard {}@{}", jobs.length, shardId, baseTimestamp);
    }

    void cancel() {
      LOGGER.warn("Cancelling shard thread: {}@{}", shardId, baseTimestamp);
      for (int i = 0; i < jobs.length; i++) {
        Job job = jobs[i];
        if (job != null) {
          try {
            job.interrupt();
          } catch (Exception e) {
            LOGGER.warn("Expected interrupt exception", e);
          }
          flushed += job.flushed;
          ooo += job.ooo;
          failures += job.failures;
          noSegment += job.noSegment;
          jobs[i] = null;
        }
      }
      double timeTaken = ((double) System.nanoTime() - (double) start) / (double) 1_000_000;
      totalTimer.stop(start, tagSet);
      flushedGuage.set(flushed, tagSet);
      oooGuage.set(ooo, tagSet);
      failureGuage.set(failures, tagSet);
      emptyGuage.set(noSegment, tagSet);
      flushInfos[shardId] = null;
      LOGGER.warn("Canceled shard thread: {}@{}", shardId, baseTimestamp);
      this.inProgress.set(false);
    }

    void complete() {
      LOGGER.debug(
          "Finishing shard flush for {}@{} with {} records", shardId, baseTimestamp, flushed);
      flushedGuage.set(flushed, tagSet);
      oooGuage.set(ooo, tagSet);
      failureGuage.set(failures, tagSet);
      emptyGuage.set(noSegment, tagSet);
      double timeTaken = ((double) System.nanoTime() - (double) start) / (double) 1_000_000;
      totalTimer.stop(start, tagSet);
      synchronized (flushInfos) {
        flushInfos[shardId] = null;
      }
      this.inProgress.set(false);
      LOGGER.info("Finished flushing shard [{}] in {}ms", shardId, timeTaken);
    }

    /** A single thread that runs until the iterator is finished. */
    class Job extends Thread {
      private final int idx;
      private final TimeSeriesRecord record;
      private final BasicTimeSeriesEncoder encoder;
      private int flushed;
      private int ooo;
      private int failures;
      private int noSegment;

      Job(final int idx) {
        this.idx = idx;
        this.record = recordFactory.create();
        this.encoder = encoderFactory.create();
      }

      @Override
      public void run() {
        Thread.currentThread().setName("Flusher_" + shardId + "@" + baseTimestamp);
        LOGGER.info("Start job thread {} for {}@{}", idx, shardId, baseTimestamp);
        while (true) {
          try {
            long hash = 0;
            long addr = 0;
            // TODO - how to avoid this?
            synchronized (iterator) {
              if (iterator.hasNext()) {
                iterator.next();
                hash = iterator.key();
                addr = iterator.value();
              }
            }

            if (addr == 0) {
              // done
              synchronized (jobs) {
                FlushInfo.this.flushed += flushed;
                FlushInfo.this.ooo += ooo;
                FlushInfo.this.failures += failures;
                jobs[idx] = null;
                int nulls = 0;
                for (int i = 0; i < jobs.length; i++) {
                  if (jobs[i] != null) {
                    break;
                  }
                  nulls++;
                }

                if (nulls == jobs.length) {
                  complete();
                  LOGGER.info(
                      "Finished flushing segments for shard {}@{}",
                      flushed,
                      shardId,
                      baseTimestamp);
                  return;
                }
              }
              LOGGER.debug("Completing job {} for {}@{}", idx, shardId, baseTimestamp);
              return;
            }

            // have some work to do, so do it!
            record.open(addr);
            long segmentAddress = record.getSegmentAddress(baseTimestamp);
            if (segmentAddress == 0) {
              // no segment for this slice.
              noSegment++;
              continue;
            }

            encoder.openSegment(segmentAddress);
            if (dsEncoderFactory != null) {
              double[] values = tlValues.get();
              Arrays.fill(values, Double.NaN);
              encoder.readAndDedupe(values);

              int segmentTime = encoder.getSegmentTime();
              DownSamplingTimeSeriesEncoder dsEncoder = dsEncoders.get();
              try {
                dsEncoder.createSegment(segmentTime);
                dsEncoder.addDataPoints(values);
                // send downstream
                for (int i = 0; i < stores.size(); i++) {
                  stores.get(i).flush(hash, dsEncoder);
                }
              } catch (Exception e) {
                LOGGER.error(
                    "Failed to flush down sampled segment for {}@{}", shardId, baseTimestamp, e);
                failures++;
              } finally {
                dsEncoder.freeSegment();
              }
            } else {
              if (encoder.segmentHasOutOfOrderOrDuplicates()) {
                BasicTimeSeriesEncoder reWrite = encoders.get();
                try {
                  double[] values = tlValues.get();
                  Arrays.fill(values, Double.NaN);
                  encoder.readAndDedupe(values);

                  int segmentTime = encoder.getSegmentTime();
                  reWrite.createSegment(segmentTime);
                  for (int i = 0; i < values.length; i++) {
                    if (!Double.isNaN(values[i])) {
                      reWrite.addDataPoint(segmentTime + i, values[i]);
                    }
                  }

                  // send downstream
                  for (int i = 0; i < stores.size(); i++) {
                    stores.get(i).flush(hash, reWrite);
                  }
                } catch (Exception e) {
                  LOGGER.error(
                      "Failed to flush an OOO segment for {}@{}", shardId, baseTimestamp, e);
                  failures++;
                } finally {
                  // CRITICAL!
                  reWrite.freeSegment();
                }
                ooo++;
              } else {
                // send downstream
                for (int i = 0; i < stores.size(); i++) {
                  stores.get(i).flush(hash, encoder);
                }
              }
            }
            flushed++;
          } catch (Throwable t) {
            failures++;
            LOGGER.error("Failed to flush a segment for {}@{}", shardId, baseTimestamp, t);
          }
        }
      }
    }
  }

  private class TSFlusherStatus implements FlushStatus {

    private final AtomicBoolean inProgress;

    public TSFlusherStatus(AtomicBoolean inProgress) {
      this.inProgress = inProgress;
    }

    @Override
    public boolean inProgress() {
      return inProgress.get();
    }
  }
}
