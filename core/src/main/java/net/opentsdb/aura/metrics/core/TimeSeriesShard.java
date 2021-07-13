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

import net.opentsdb.aura.metrics.core.coordination.Clock;
import net.opentsdb.aura.metrics.core.coordination.Gate;
import net.opentsdb.aura.metrics.core.coordination.GatedJobWrapper;
import net.opentsdb.aura.metrics.core.coordination.Job;
import net.opentsdb.aura.metrics.core.coordination.JobWrapper;
import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.data.HashTable;
import net.opentsdb.aura.metrics.core.data.InSufficientBufferLengthException;
import net.opentsdb.aura.metrics.core.data.Memory;
import net.opentsdb.aura.metrics.core.data.ResultantPointerArray;
import net.opentsdb.aura.metrics.meta.MetaDataStore;
import net.opentsdb.aura.metrics.meta.MetaQuery;
import net.opentsdb.aura.metrics.meta.MetricQuery;
import net.opentsdb.aura.metrics.meta.Query;
import net.opentsdb.aura.metrics.meta.SharedMetaResult;
import io.ultrabrew.metrics.Counter;
import io.ultrabrew.metrics.Gauge;
import io.ultrabrew.metrics.MetricRegistry;
import io.ultrabrew.metrics.Timer;
import net.opentsdb.collections.LongIntHashTable;
import net.opentsdb.collections.LongLongHashTable;
import net.opentsdb.collections.LongLongIterator;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.hashing.HashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static net.opentsdb.aura.metrics.core.StorageMode.EPHEMERAL;
import static net.opentsdb.aura.metrics.core.StorageMode.LONG_RUNNING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TimeSeriesShard implements TimeSeriesShardIF {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesShard.class);

  public static final int PURGE_JOB_TIMEOUT_MS = 100;
  private final ShardConfig shardConfig;
  private final TimeseriesStorageContext storageContext;
  private final HashFunction hashFunction;

  private final int secondsInASegment;
  private final int secondsInATimeSeries;
  private final int segmentsInATimeSeries;

  private final int shardId;
  private final BasicTimeSeriesEncoder encoder;
  private final LinkedBlockingQueue<Runnable> queue;
  private final HashTable tagTable;
  private final HashTable metricTable;
  private final LongLongHashTable timeSeriesTable;
  private final LongIntHashTable metaCountTable;
  private final int retentionSeconds;
  private final StorageMode storageMode;

  private final TimeSeriesRecord timeSeriesRecord;

  private final MetaDataStore docStore;

  private Timer timeSeriesWriteTimer;
  private Timer shardReadTimer;
  private Counter timeSeriesWriteFailureCounter;
  private Gauge dataPointCountGauge;
  private Gauge metricLengthGauge;
  private Gauge metricCountGauge;
  private Gauge tagsetLengthGauge;
  private Gauge tagsetCountGauge;
  private Gauge metaRecordCountGauge;
  private Gauge timeSeriesCountGauge;
  private Counter timeSeriesDropCounter;
  private Counter timeSeriesPurgeCounter;
  private Timer timeSeriesPurgeTimer;
  private Timer metaSearchTimer;
  private Timer metaWriteTimer;
  private Timer metaPurgeTimer;
  private Counter metaPurgeFailureCounter;
  private String[] tagSet;
  private Flusher flusher;
  private final ScheduledExecutorService scheduledExecutorService;
  private MemoryInfoReader memoryInfoReader;

  private long dataPointCount = 0;
  private long dataDropCount = 0;
  private long timeSeriesCount = 0;
  private long metricLengthBytes = 0;
  private long tagsetLengthBytes = 0;

  private final byte[] pointerBuffer = new byte[HashTable.valSz + 4];
  private byte[] byteBuffer = new byte[8];
  private long[] docIdBuffer = new long[8];
  private ResultantPointerArray queryResult = new ResultantPointerArray(0);
  private double memoryUsageLimit;
  private int maxTagLength;
  private int maxMetricLength;
  private volatile PurgeJob purgeJob;
  private final long[] docIdBatch; // = new long[DOCID_BATCH_SIZE]; // L2 cache aligned.
  private final ReadWriteLock purgeLock = new ReentrantReadWriteLock();
  private final Gate purgeFlushGate = new Gate();

  private final LinkedBlockingQueue<JobWrapper> gatedJobsQueue = new LinkedBlockingQueue<>();

  public TimeSeriesShard(
      final int shardId,
      final ShardConfig config,
      final TimeseriesStorageContext storageContext,
      final BasicTimeSeriesEncoder encoder,
      final MetaDataStore docStore,
      final MemoryInfoReader memoryInfoReader,
      final MetricRegistry metricRegistry,
      final LocalDateTime purgeDateTime,
      final HashFunction hashFunction,
      final Flusher flusher,
      final ScheduledExecutorService scheduledExecutorService) {

    this.shardConfig = config;
    this.storageContext = storageContext;
    this.hashFunction = hashFunction;
    this.segmentsInATimeSeries = storageContext.getSegmentsInATimeSeries();
    this.secondsInASegment = storageContext.getSecondsInASegment();
    this.secondsInATimeSeries = storageContext.getSecondsInATimeSeries();
    this.shardId = shardId;
    this.encoder = encoder;
    this.queue = new LinkedBlockingQueue(config.queueSize);
    this.docStore = docStore;
    this.tagSet =
        new String[] {"namespace", shardConfig.namespace, "shardId", String.valueOf(shardId)};
    this.flusher = flusher;
    this.scheduledExecutorService = scheduledExecutorService;
    this.encoder.setTags(tagSet);
    this.memoryInfoReader = memoryInfoReader;
    this.memoryUsageLimit = shardConfig.memoryUsageLimitPct;
    this.retentionSeconds = (int) TimeUnit.HOURS.toSeconds(shardConfig.retentionHour);
    this.timeSeriesRecord = new OffHeapTimeSeriesRecord(segmentsInATimeSeries, secondsInASegment);
    this.storageMode = storageContext.getMode();

    this.tagTable = new HashTable("TagTable" + shardId, shardConfig.tagTableSize);
    this.metricTable = new HashTable("MetricTable" + shardId, shardConfig.metricTableSize);
    this.timeSeriesTable =
        new LongLongHashTable(shardConfig.timeSeriesTableSize, "TSTable" + shardId);
    this.metaCountTable =
        new LongIntHashTable(shardConfig.timeSeriesTableSize, "MetaCountTable" + shardId);
    this.docIdBatch = new long[shardConfig.metaPurgeBatchSize];

    this.shardReadTimer = metricRegistry.timer("shard.read.time");
    this.timeSeriesWriteTimer = metricRegistry.timer("timeseries.write.timer");
    this.timeSeriesPurgeTimer = metricRegistry.timer("timeseries.purge.timer");
    this.timeSeriesPurgeCounter = metricRegistry.counter("timeseries.purge.count");
    this.timeSeriesWriteFailureCounter = metricRegistry.counter("timeseries.write.failure.count");
    this.timeSeriesCountGauge = metricRegistry.gauge("timeseries.count");
    this.timeSeriesDropCounter = metricRegistry.counter("timeseries.drop.count");
    this.dataPointCountGauge = metricRegistry.gauge("datapoint.count");
    this.metricLengthGauge = metricRegistry.gauge("metric.length");
    this.metricCountGauge = metricRegistry.gauge("metric.count");
    this.tagsetLengthGauge = metricRegistry.gauge("tagset.length");
    this.tagsetCountGauge = metricRegistry.gauge("tagset.count");
    this.metaWriteTimer = metricRegistry.timer("meta.write.timer");
    this.metaPurgeTimer = metricRegistry.timer("meta.purge.time");
    this.metaSearchTimer = metricRegistry.timer("meta.search.time");
    this.metaPurgeFailureCounter = metricRegistry.counter("meta.purge.failure.count");
    this.metaRecordCountGauge = metricRegistry.gauge("meta.record.count");

    Thread t =
        new Thread(
            () -> {
              while (true) {
                try {
                  queue.take().run();
                } catch (Throwable ignored) {
                  LOGGER.error("Error in shard thread", ignored);
                }
              }
            });
    t.setName("Shard thread " + shardId);
    t.start();
    this.flusher = flusher;

    if (storageMode == LONG_RUNNING) {
        Function<Runnable, Boolean> runnableConsumer = runnable -> {
            try {
                submit(runnable);
                return true;
            } catch (InterruptedException e) {
                LOGGER.error("Failed to submit initial purge job on shard " + shardId, e);
                return false;
            }
        };
        LocalDateTime now = LocalDateTime.now();
        long initialDelay = now.until(purgeDateTime, ChronoUnit.SECONDS);
        initJob(new GatedJobWrapper(
                null,
                new PurgeJob(),
                new Clock(initialDelay, shardConfig.purgeFrequencyMinutes * 60),
                purgeFlushGate,
                Gate.KEY.PURGE,
                runnableConsumer
        ));
        if (flusher != null) {
          initJob(new GatedJobWrapper(
                  null,
                  new Flush(flusher),
                  new Clock(flusher.frequency(), (int) flusher.frequency()),
                  purgeFlushGate,
                  Gate.KEY.FLUSH,
                  runnableConsumer
          ));
        }
        //TODO: Abstract this out too
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                List<JobWrapper> jobWrapperList = new ArrayList<>();
                JobWrapper jobWrapper;
                while ((jobWrapper = this.gatedJobsQueue.poll(100, MILLISECONDS)) != null) {
                  LOGGER.info("Trying to run: {} for shard: {}", jobWrapper, shardId);
                    if (jobWrapper.tryToRun()) {
                      LOGGER.info("Submitted: {} for shard: {}", jobWrapper, shardId);
                      JobWrapper next = jobWrapper.createNext();
                      LOGGER.info("Created: {} and putting it in the queue for shard: {}", next, shardId);
                      jobWrapperList.add(next);
                    } else {
                      jobWrapperList.add(jobWrapper);
                    }
                }
                for(JobWrapper j : jobWrapperList) {
                  this.gatedJobsQueue.put(j);
                }
            } catch (Throwable e) {
                LOGGER.error("Purge flush thread interrupted", e);
            }
        }, 180, 180, SECONDS);
    }


    int segmentCollectionFrequencySeconds = shardConfig.segmentCollectionFrequencySeconds;
    long segmentCollectionFrequencyMillis =
        TimeUnit.SECONDS.toMillis(segmentCollectionFrequencySeconds);

    Thread gt =
        new Thread(
            () -> {
              while (true) {
                try {
                  submit(() -> encoder.freeCollectedSegments());
                  Thread.sleep(segmentCollectionFrequencyMillis);
                } catch (InterruptedException e) {
                  LOGGER.error("Segment Collection thread interrupted");
                }
              }
            });
    gt.setDaemon(true);
    gt.setName("Segment Collection thread " + shardId);
    gt.start();
    LOGGER.info(
        "Running segment collection for shard: {} frequency: {} seconds",
        shardId,
        segmentCollectionFrequencySeconds);
  }


  private void initJob(JobWrapper jobWrapper) {
    try {
      this.gatedJobsQueue.put(jobWrapper);
    } catch (InterruptedException e) {
      throw new RuntimeException("Unable to instantiate job: " + jobWrapper, e);
    }
  }

  private class Flush implements Job {

    private final Flusher flusher;

    private volatile FlushStatus flushStatus;
    private volatile LongLongHashTable tsTableClone;
    public Flush(Flusher flusher) {

      this.flusher = flusher;
    }

    @Override
    public void run() {
      try {
        // clone time series table
        int ts = (int) (System.currentTimeMillis() / 1000);
        // TEMP - this is so we can flush old data
        ts -= flusher.frequency();
        ts = storageContext.getSegmentTime(ts);
        LOGGER.info("Starting shard flush on " + shardId + "@" + ts + " frequency in seconds: "+ flusher.frequency());
        long start = System.nanoTime();
        tsTableClone = (LongLongHashTable) timeSeriesTable.clone();
        final HashTable tagTableClone = tagTable.clone();
        final HashTable metricTableClone = metricTable.clone();
        double timeTaken = ((double) (System.nanoTime() - start) / (double) 1_000_000);
        LOGGER.info("Finished cloning tables for " + shardId + "@" + ts + " in " + timeTaken + "ms");
        //Should pass clones
        this.flushStatus = flusher.flushShard(shardId, tagTableClone, metricTableClone, tsTableClone, ts);
      } catch (Throwable throwable) {
        LOGGER.error("Error flushing", throwable);
      }
    }

    @Override
    public boolean isComplete() {
      return this.flushStatus != null && !this.flushStatus.inProgress();
    }

    @Override
    public Job createNext() {
      return new Flush(this.flusher);
    }

    @Override
    public String toString() {
      final String status = flushStatus == null ? "Not started" : (flushStatus.inProgress() ? "In progress" : "Completed");
      return "Flush job status: " + status;
    }

    @Override
    public void close() {
      if(isComplete()) {
        tsTableClone.close();
      }
    }
  }

  @Override
  public void flush() {
    if(this.flusher != null) {
      Flush flush = new Flush(flusher);
      flush.run();
    }
  }

  @Override
  public void submit(Runnable job) throws InterruptedException {
    queue.put(job);
  }

  @Override
  public int getAndRemoveTagset(long tagKey, byte[] byteBuffer)
      throws InSufficientBufferLengthException {
    tagTable.getPointer2(tagKey, pointerBuffer);
    long tagAddr = ByteArrays.getLong(pointerBuffer, 0);
    int tagLen = ByteArrays.getInt(pointerBuffer, 8);
    int slot = ByteArrays.getInt(pointerBuffer, 12);

    if (tagLen > byteBuffer.length) {
      throw new InSufficientBufferLengthException(tagLen, byteBuffer.length);
    }
    Memory.read(tagAddr, byteBuffer, 0, tagLen);
    // remove entry from tag table
    tagTable.resetSlot(slot);
    Memory.free(tagAddr);
    tagsetLengthBytes -= tagLen;
    return tagLen;
  }

  @Override
  public long getDataPointCount() {
    return dataPointCount;
  }

  @Override
  public long getDataDropCount() {
    return dataDropCount;
  }

  @Override
  public int getId() {
    return shardId;
  }

  @Override
  public void addEvent(final LowLevelMetricData.HashedLowLevelMetricData event) {
    try {
      while (event.advance()) {
        int segmentTime;
        int timestamp;
        try {
          timestamp = (int) event.timestamp().epoch();
          long start = timeSeriesWriteTimer.start();
          addTimeSeries(event, timestamp,
                  storageContext.getSegmentTime(timestamp));
          timeSeriesWriteTimer.stop(start, tagSet);
        } catch (Throwable t) {
          LOGGER.error("Error storing timeseries", t);
          timeSeriesWriteFailureCounter.inc(tagSet);
        }
      }

      dataPointCountGauge.set(dataPointCount, tagSet);
      timeSeriesCountGauge.set(timeSeriesCount, tagSet);
      metricLengthGauge.set(metricLengthBytes, tagSet);
      metricCountGauge.set(metricTable.size(), tagSet);
      tagsetLengthGauge.set(tagsetLengthBytes, tagSet);
      tagsetCountGauge.set(tagTable.size(), tagSet);
      metaRecordCountGauge.set(docStore.size(), tagSet);
      encoder.collectMetrics();
    } finally {
      try {
        event.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close event", e);
      }
    }
  }

  /**
   * This method is not to be used with long running purge.
   */
  @Override
  public void purge() {
    if(storageMode == EPHEMERAL) {
      if (purgeJob == null) {
        purgeJob = new PurgeJob();
        try {
          submit(purgeJob);
        } catch (InterruptedException e) {
          LOGGER.error("Failed to submit initial purge job on shard " + shardId, e);
        }
      } else {
        LOGGER.warn("A purge job is still running on shard " + shardId);
      }
    }
  }

  public class PurgeJob implements Job {

    long start = timeSeriesPurgeTimer.start();
    LongLongIterator tsIterator = timeSeriesTable.iterator();
    int currentTimeInSeconds = (int) (System.currentTimeMillis() / 1000);
    int segmentPurgeCount = 0;
    int dpPurgeCount = 0;
    int metaPurgeCount = 0;
    int tsPurgeCount = 0;
    int rescheduleAttempts = 0;
    final AtomicBoolean isPurgeDone = new AtomicBoolean(false);
    void reschedule() {
      if (!queue.offer(this)) {
        rescheduleAttempts++;
        if (rescheduleAttempts > 1024) {
          LOGGER.error("Tried to reschedule a purge on shard " + shardId + " too many times");
          purgeJob = null;
          isPurgeDone.set(true);
          return;
        }
        scheduledExecutorService.schedule(() -> reschedule(), 1, TimeUnit.SECONDS);
      }
    }

    @Override
    public void run() {
      LOGGER.info("Purge job started for shard: {}", shardId);
      long jobStart = System.currentTimeMillis();
      rescheduleAttempts = 0;
      Arrays.fill(docIdBatch, 0l);
      int batchSize = 0;
      while (tsIterator.hasNext()) {
        tsIterator.next();

        long tsAddress = tsIterator.value();
        timeSeriesRecord.open(tsAddress);

        int lastTimestamp = timeSeriesRecord.getLastTimestamp();
        int secondsSinceLastUpdate = currentTimeInSeconds - lastTimestamp;

        if (secondsSinceLastUpdate
            >= secondsInASegment) { // timestamp is not updated since last two hours. scan for stale
          // segments.
          final int currentSegmentTime = storageContext.getSegmentTime(currentTimeInSeconds);
          final int lastUpdatedSegmentTime = storageContext.getSegmentTime(lastTimestamp);
          final int lastUpdatedSegmentIndex =
              timeSeriesRecord.getSegmentIndex(lastUpdatedSegmentTime);

          for (int j = lastUpdatedSegmentIndex;
              j < lastUpdatedSegmentIndex + segmentsInATimeSeries;
              j++) {
            int segmentIndex = j % segmentsInATimeSeries;
            long segmentAddress = timeSeriesRecord.getSegmentAddressAtIndex(segmentIndex);
            if (segmentAddress != 0) {
              encoder.openSegment(segmentAddress);
              if (currentSegmentTime - encoder.getSegmentTime()
                  > retentionSeconds) { // segment older than 26 hours, delete it.
                int numPoints = encoder.getNumDataPoints();
                encoder.freeSegment();
                dpPurgeCount += numPoints; // to decrement data point count
                timeSeriesRecord.deleteSegmentAddressAtIndex(segmentIndex);
                segmentPurgeCount++;
              }
            }
          }
        }

        if (secondsSinceLastUpdate
            >= secondsInATimeSeries) { // timeseries older than 26 hours, delete it.

          long tagKey = timeSeriesRecord.getTagKey();
          int metaCount = metaCountTable.get(tagKey);
          metaCount--;
          metaCountTable.put(tagKey, metaCount);

          // if the meta count is 0, remove it from the meta store
          if (metaCount == 0) {
            docIdBatch[batchSize++] = tagKey;
            metaCountTable.remove(tagKey);
          }

          timeSeriesRecord.delete();
          tsIterator.remove(); // removes the entry from the table
          tsPurgeCount++;
        }

        // if batch is full or it's the last batch, then remove it from the doc store.
        if (batchSize == docIdBatch.length || (!tsIterator.hasNext() && batchSize > 0)) {

          if (byteBuffer.length < maxTagLength) {
            byteBuffer = new byte[maxTagLength];
          }

          try {
            long s = metaPurgeTimer.start();
            docStore.remove(docIdBatch, batchSize, byteBuffer);
            metaPurgeTimer.stop(s, tagSet);
            metaPurgeCount += batchSize;
          } catch (InSufficientBufferLengthException e) {
            // should not happen
            LOGGER.error("Error removing docIds from doc store", e);
            metaPurgeFailureCounter.inc(tagSet);
          }
        }

        if (System.currentTimeMillis() - jobStart >= PURGE_JOB_TIMEOUT_MS && tsIterator.hasNext()) {
          // reschedule
          if (!queue.offer(this)) {
            LOGGER.info("Purge timeout reached for shardId {} rescheduling", shardId);
            rescheduleAttempts++;
            scheduledExecutorService.schedule(() -> reschedule(), 1, TimeUnit.SECONDS);
          }
          return;
        }
      }

      timeSeriesPurgeTimer.stop(start, tagSet);
      timeSeriesPurgeCounter.inc(tsPurgeCount, tagSet);
      timeSeriesCount -= tsPurgeCount;
      dataPointCount -= dpPurgeCount;

      timeSeriesCountGauge.set(timeSeriesCount, tagSet);
      dataPointCountGauge.set(dataPointCount, tagSet);
      tagsetLengthGauge.set(tagsetLengthBytes, tagSet);
      tagsetCountGauge.set(tagTable.size(), tagSet);
      metaRecordCountGauge.set(docStore.size(), tagSet);
      encoder.collectMetrics();

      LOGGER.info(
          "Purged {} segments {} timeseries {} meta records for shardId {}",
          segmentPurgeCount,
          tsPurgeCount,
          metaPurgeCount,
          shardId);
      purgeJob = null;
      isPurgeDone.set(true);
    }


    @Override
    public boolean isComplete() {
      return isPurgeDone.get();
    }

    @Override
    public Job createNext() {
      return new PurgeJob();
    }

    @Override
    public void close() {
      //Do nothing
    }

    @Override
    public String toString() {
      final String status = !isPurgeDone.get() ? "Not started/In progress" : "Completed";
      return "Purge job status: " + status;
    }
  }

  @Override
  public void read(
      final int startTime,
      final int endTime,
      final long timeSeriesHash,
      final TSDataConsumer consumer) {

    final long segmentTimeInfo = storageContext.getSegmentTimes(startTime, endTime);
    final int firstSegmentTime = segmentTimeInfo > 0 ? storageContext.getFirstSegmentTime(segmentTimeInfo) : 0;
    final int segmentCount = segmentTimeInfo > 0 ? storageContext.getSegmentCount(segmentTimeInfo) : 0;
    long tsRecordAddress = timeSeriesTable.get(timeSeriesHash);
    if (tsRecordAddress != timeSeriesTable.NOT_FOUND) {
      timeSeriesRecord.open(tsRecordAddress);

      int segmentTime = firstSegmentTime;
      int count = 0;
      while(count < segmentCount) {
        long segmentAddress = timeSeriesRecord.getSegmentAddress(segmentTime);
        if (segmentAddress > 0) { // segment found
          encoder.openSegment(segmentAddress);
          if (encoder.getSegmentTime() == segmentTime) {
            encoder.read(consumer);
          }
        }
        count++;
      }
    }
  }

  @Override
  public long query(final MetricQuery metricQuery, final int firstSegmentTime, final int segmentCount) throws Exception {
    long st = shardReadTimer.start();
    int cardinality;
    Query query = metricQuery.getQuery();
    try {
      long start = metaSearchTimer.start();
      cardinality = docStore.search(query, metricQuery.getMetricString(), docIdBuffer);
      metaSearchTimer.stop(start, tagSet);
    } catch (MetaDataStore.InSufficientArrayLengthException e) {
      int required = e.getRequired();
      int newLength = required * 2;
      int oldLength = e.getFound();
      LOGGER.warn(
          "Growing doc id buffer for shard: {} from: {} to: {}", shardId, oldLength, newLength);
      docIdBuffer = new long[newLength];
      long start = metaSearchTimer.start();
      cardinality = docStore.search(query, metricQuery.getMetricString(), docIdBuffer);
      metaSearchTimer.stop(start, tagSet);
    }

    queryResult.create(cardinality);
    if (cardinality <= 0) {
      shardReadTimer.stop(st, tagSet);
      // empty result
      return 0;
    }

    byte[] metric = metricQuery.getMetric();
    long metricHash = hashFunction.hash(metric);

    int tsIndex = 0;
    for (int i = 0; i < cardinality; i++) {
      long tagHash = docIdBuffer[i];
      long seriesHash = hashFunction.update(metricHash, tagHash);
      long tsRecordAddress = timeSeriesTable.get(seriesHash);
      if (tsRecordAddress == timeSeriesTable.NOT_FOUND) {
        continue;
      }
      timeSeriesRecord.open(tsRecordAddress);
      if (query.isExactMatch()) {
        byte actualTagCount = timeSeriesRecord.getTagCount();
        if (actualTagCount != query.getTagCount()) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Skip tagHash: {} metric: {} tsKey: {} expectedTagCount: {} actualTagCount: {} ",
                tagHash,
                new String(metric),
                seriesHash,
                query.getTagCount(),
                actualTagCount);
          }
          continue;
        }
      }

      boolean segmentFound = false;
      int segmentTime = firstSegmentTime;
      int count = 0;
      while(count < segmentCount) {
        long segmentAddress = timeSeriesRecord.getSegmentAddress(segmentTime);
        if (segmentAddress > 0) {
          // timeseries has data for this query range. So, take it in.
          segmentFound = true;
          break;
        }
        segmentTime += storageContext.getSecondsInASegment();
        count++;
      }

      if (segmentFound) {
        getTagPointer(timeSeriesRecord.getTagKey(), pointerBuffer);
        long tagAddress = ByteArrays.getLong(pointerBuffer, 0);
        int tagLength = ByteArrays.getInt(pointerBuffer, 8);
        queryResult.set(tsIndex++, tsRecordAddress, tagAddress, tagLength);
      }
    }
    queryResult.setTSCount(tsIndex);
    shardReadTimer.stop(st, tagSet);
    return queryResult.getAddress();
  }

  @Override
  public long query(MetricQuery metricQuery) throws Exception {
    int cardinality;
    Query query = metricQuery.getQuery();
    try {
      long start = metaSearchTimer.start();
      cardinality = docStore.search(query, metricQuery.getMetricString(), docIdBuffer);
      metaSearchTimer.stop(start, tagSet);
    } catch (MetaDataStore.InSufficientArrayLengthException e) {
      int required = e.getRequired();
      int newLength = required * 2;
      int oldLength = e.getFound();
      LOGGER.warn(
          "Growing doc id buffer for shard: {} from: {} to: {}", shardId, oldLength, newLength);
      docIdBuffer = new long[newLength];
      long start = metaSearchTimer.start();
      cardinality = docStore.search(query, metricQuery.getMetricString(), docIdBuffer);
      metaSearchTimer.stop(start, tagSet);
    }

    if (cardinality <= 0) {
      return 0;
    }

    queryResult.create(cardinality);
    long metricHash = hashFunction.hash(metricQuery.getMetric());
    int tsIndex = 0;
    for (int i = 0; i < cardinality; i++) {
      long tagHash = docIdBuffer[i];
      long seriesHash = hashFunction.update(metricHash, tagHash);
      long tsRecordAddress = timeSeriesTable.get(seriesHash);
      if (tsRecordAddress == timeSeriesTable.NOT_FOUND) {
        continue;
      }
      timeSeriesRecord.open(tsRecordAddress);
      byte actualTagCount = timeSeriesRecord.getTagCount();
      if (query.isExactMatch() && actualTagCount != query.getTagCount()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Skip tagHash: {} metric: {} tsKey: {} expectedTagCount: {} actualTagCount: {} ",
              tagHash,
              new String(metricQuery.getMetric()),
              seriesHash,
              query.getTagCount(),
              actualTagCount);
        }
        continue;
      }
      getTagPointer(timeSeriesRecord.getTagKey(), pointerBuffer);
      long tagAddress = ByteArrays.getLong(pointerBuffer, 0);
      int tagLength = ByteArrays.getInt(pointerBuffer, 8);
      queryResult.set(tsIndex++, tsRecordAddress, tagAddress, tagLength);
    }
    queryResult.setTSCount(tsIndex);
    return queryResult.getAddress();
  }

  @Override
  public void readAll(long timeSeriesHash, TSDataConsumer consumer) {
    long tsRecordAddress = timeSeriesTable.get(timeSeriesHash);
    if (tsRecordAddress != timeSeriesTable.NOT_FOUND) {
      timeSeriesRecord.open(tsRecordAddress);
      for (int i = 0; i < segmentsInATimeSeries; i++) {
        long segmentAddress = timeSeriesRecord.getSegmentAddressAtIndex(i);
        if (segmentAddress > 0) { // segment found
          encoder.openSegment(segmentAddress);
          encoder.read(consumer);
        }
      }
    }
  }

  @Override
  public SharedMetaResult queryMeta(MetaQuery query) {
    return docStore.queryMeta(query);
  }

  @Override
  public byte getTagCount(long timeSeriesKey) {
    long tsRecordAddress = timeSeriesTable.get(timeSeriesKey);
    if (tsRecordAddress == timeSeriesTable.NOT_FOUND) {
      return NOT_FOUND;
    } else {
      timeSeriesRecord.open(tsRecordAddress);
      return timeSeriesRecord.getTagCount();
    }
  }

  @Override
  public boolean getTagPointer(final long tagKey, final byte[] buffer) {
    return tagTable.getPointer(tagKey, buffer);
  }

  private void addTimeSeries(
      final LowLevelMetricData.HashedLowLevelMetricData event, final int timestamp, final int segmentTime) {
    long seriesHash = event.timeSeriesHash();
    long tagKey = event.tagsSetHash();
    final double value;
    switch (event.valueFormat()) {
      case INTEGER:
        value = event.longValue();
        break;
      case FLOAT:
        value = event.floatValue();
        break;
      case DOUBLE:
        value = event.doubleValue();
        break;
      default:
        throw new IllegalArgumentException("Unexpected value type: " + event.valueFormat());
    }

    long tsRecordAddress = timeSeriesTable.get(seriesHash);
    if (tsRecordAddress == timeSeriesTable.NOT_FOUND) { // add new time series

      if (memoryUsageLimit > 0 && memoryInfoReader.getTotalMemoryUsage() >= memoryUsageLimit) {
        timeSeriesDropCounter.inc(tagSet);
        dataDropCount++;
        return;
      }

      int count;
      if (!tagTable.containsKey(tagKey)) {
        int length = event.tagBufferLength();
        tagTable.put(event.tagsSetHash(), event.tagsBuffer(), event.tagBufferStart(), length);
        tagsetLengthBytes += length;

        long start = metaWriteTimer.start();
        docStore.add(event);
        metaWriteTimer.stop(start, tagSet);
        count = 1;
        metaCountTable.put(tagKey, count);

        if (length > maxTagLength) {
          maxTagLength = length;
        }
      } else {
        if (shardConfig.metaQueryEnabled) {
          docStore.addMetric(event);
          count = metaCountTable.get(tagKey) + 1;
          metaCountTable.put(tagKey, count);
        }
      }

      long metricKey = event.metricHash();
      if (!metricTable.containsKey(metricKey)) {
        int length = event.metricLength();
        metricTable.put(metricKey, event.metricBuffer(), event.metricStart(), length);
        metricLengthBytes += length;
        if (length > maxMetricLength) {
          maxMetricLength = length;
        }
      }

      final long segmentAddress = encoder.createSegment(segmentTime);
      final long tsAddress =
          timeSeriesRecord.create(
              metricKey,
              tagKey,
              (byte) event.tagSetCount(),
              timestamp,
              value,
              segmentTime,
              segmentAddress);
      timeSeriesTable.put(seriesHash, tsAddress);
      timeSeriesCount++;
    } else { // existing time series
      timeSeriesRecord.open(tsRecordAddress);
      final int segmentIndex = timeSeriesRecord.getSegmentIndex(segmentTime);
      long segmentAddress = timeSeriesRecord.getSegmentAddressAtIndex(segmentIndex);
      if (segmentAddress == 0) { // add a new segment
        segmentAddress = encoder.createSegment(segmentTime);
        timeSeriesRecord.setSegmentAddressAtIndex(segmentIndex, segmentAddress);

      } else { // update an existing segment

        encoder.openSegment(segmentAddress);
        int currentSegmentTime = encoder.getSegmentTime();
        if (currentSegmentTime != segmentTime) { // drop old segment and create a new one.
          encoder.collectSegment(segmentAddress);
          int size = encoder.getNumDataPoints();
          dataPointCount -= size; // decrement data point count

          segmentAddress = encoder.createSegment(segmentTime);
          timeSeriesRecord.setSegmentAddressAtIndex(segmentIndex, segmentAddress);
        }
      }
    }

    encoder.addDataPoint(timestamp, value);

    int lastTimestamp = timeSeriesRecord.getLastTimestamp();
    if (lastTimestamp <= timestamp) {
      timeSeriesRecord.setLastTimestamp(timestamp);
      timeSeriesRecord.setLastValue(value);
    }
    dataPointCount++;
  }

}
