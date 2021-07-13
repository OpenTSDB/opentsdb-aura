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

package net.opentsdb.query.processor.groupby;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.core.LongTermStorage;
import net.opentsdb.aura.metrics.core.BasicTimeSeriesEncoder;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.storage.AerospikeQueryNode;
import net.opentsdb.aura.metrics.storage.AerospikeGBQueryResult;
import net.opentsdb.aura.metrics.storage.AuraMetricsNumericArrayIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import net.opentsdb.common.Const;
import net.opentsdb.data.*;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.pools.*;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.utils.BigSmallLinkedBlockingQueue;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * TODO - handle rates on the segment boundary wherin we could fetch one segment
 * earlier to try and find a proper rate instead of returning double[] {NaN, val, val...}
 *
 * TODO - handle downsampling infectious NaNs. We would need to know the reporting
 * interval.
 *
 * TODO - handle group by infectious NaNs as well. Right now the aggregators from
 * TSD will fill with NaN then when we set a value it'll be NaN'd out so we need
 * a bitmap or something to determine if it's the first time we're writing or not.
 */
public class AerospikeGBTimeSeries implements TimeSeries, CloseablePooledObject {
  private static final Logger LOG = LoggerFactory.getLogger(AerospikeGBTimeSeries.class);

  protected AerospikeGBQueryResult results;
  protected MetaTimeSeriesQueryResult.GroupResult metaResult;
  protected AerospikeQueryNode aerospikeQueryNode;

  /** A cached hash code ID. */
  protected volatile long cached_hash;

  protected boolean has_next;
  protected DownsampleConfig dsConfig;
  //protected final boolean infectiousNans;
  protected final AuraMetricsNumericArrayIterator.Agg dsAgg;
  protected final boolean reportingAverage;
  protected NumericAggregator nonOptimizedAggregator;
  protected PooledObject nonOptimizedPooled;
  protected double[] nonOptimizedArray;
  protected MutableNumericValue nonOptimizedDp;
  protected final int queryStartTime;
  protected final int queryEndTime;
  protected final int dsInterval;
  protected TimeSeriesId id;
  protected final NumericArrayAggregator combinedAggregator;
  protected final boolean logTrace;
  protected final boolean queryTrace;

  protected int jobCount;
  protected int totalTsCount;
  protected PooledObject[] jobs;
  protected CountDownLatch doneSignal;
  protected int aggrCount;
  protected NumericArrayAggregator[] valuesCombiner;
  protected PooledObject[] pooled_arrays;
  protected final AtomicInteger total_jobs = new AtomicInteger();

  // TEMP debug stuff
  protected String debugId;
  volatile int missingSegments;
  volatile int validSegments;
  volatile long maxASLatency;
  volatile long asLatencySum;
  volatile int asLatencyCount;
  volatile long maxSegmentRead;
  volatile long segmentReadSum;
  volatile int segmentReadCount;

  public AerospikeGBTimeSeries(
          final AerospikeGBQueryResult results,
          final MetaTimeSeriesQueryResult.GroupResult metaResult) {
    logTrace = LOG.isTraceEnabled();
    queryTrace = results.source().pipelineContext().query().isTraceEnabled();
    this.results = results;
    this.metaResult = metaResult;
    aerospikeQueryNode = (AerospikeQueryNode) results.source();
    dsConfig = aerospikeQueryNode.downsampleConfig();
    //infectiousNans = dsConfig.getInfectiousNan();
    this.queryStartTime = (int) dsConfig.startTime().epoch();
    this.queryEndTime = (int) dsConfig.endTime().epoch();
    this.dsInterval =
            dsConfig.getRunAll()
                    ? (queryEndTime - queryStartTime)
                    : (int) dsConfig.interval().get(ChronoUnit.SECONDS);

    String dsAggString = dsConfig.getAggregator().toLowerCase(Locale.ROOT);
    if (dsAggString.equalsIgnoreCase("AVG") &&
            dsConfig.dpsInInterval() > 0) {
      reportingAverage = true;
      dsAggString = "sum";
    } else {
      reportingAverage = false;
    }
    if (queryTrace) {
      debugId = results.metricString() + " " + ((TimeSeriesStringId) id()).tags();
    }

    switch (dsAggString) {
      case "sum":
      case "zimsum":
        dsAgg = AuraMetricsNumericArrayIterator.Agg.SUM;
        break;
      case "avg":
        dsAgg = AuraMetricsNumericArrayIterator.Agg.AVG;
        break;
      case "count":
        dsAgg = AuraMetricsNumericArrayIterator.Agg.COUNT;
        break;
      case "max":
      case "mimmax":
        dsAgg = AuraMetricsNumericArrayIterator.Agg.MAX;
        break;
      case "min":
      case "mimmin":
        dsAgg = AuraMetricsNumericArrayIterator.Agg.MIN;
        break;
      case "last":
        dsAgg = AuraMetricsNumericArrayIterator.Agg.LAST;
        break;
      default:
        dsAgg = AuraMetricsNumericArrayIterator.Agg.NON_OPTIMIZED;
        final NumericAggregatorFactory factory =
                results
                        .source()
                        .pipelineContext()
                        .tsdb()
                        .getRegistry()
                        .getPlugin(NumericAggregatorFactory.class, dsAggString);
        nonOptimizedAggregator = factory.newAggregator(false/*, infectiousNans*/);
        ObjectPool pool =
                results
                        .source()
                        .pipelineContext()
                        .tsdb()
                        .getRegistry()
                        .getObjectPool(DoubleArrayPool.TYPE);
        if (pool != null) {
          nonOptimizedPooled = ((ArrayObjectPool) pool).claim(64);
          nonOptimizedArray = (double[]) nonOptimizedPooled.object();
        } else {
          nonOptimizedArray = new double[64];
        }
        nonOptimizedDp = new MutableNumericValue();
    }

    combinedAggregator = ((AerospikeQueryNode) results.source()).newAggregator();

    jobCount = (int) Math.ceil((double) metaResult.numHashes() / aerospikeQueryNode.seriesPerJob());
    totalTsCount = results.metaResult().totalHashes();

    // if we don't have enough time series split into multiple jobs don't bother submitting
    // to another queue. Just crank them out here.
    if (jobCount > 1) {
      jobs = new PooledObject[jobCount];
      aggrCount = Math.min(jobCount, Math.min(metaResult.numHashes(), aerospikeQueryNode.getGbThreads()));
      doneSignal = new CountDownLatch(aggrCount);

      valuesCombiner = new NumericArrayAggregator[aggrCount];
      pooled_arrays = new PooledObject[aggrCount];
      for (int i = 0; i < valuesCombiner.length; i++) {
        valuesCombiner[i] = createAggregator(pooled_arrays, i);
      }
    }
  }

  @Override
  public TimeSeriesId id() {
    if (id == null) {
      synchronized (this) {
        if (id == null) {
          id = new TSID();
        }
      }
    }
    return id;
  }

  class GB implements GroupByFactory.GroupByJob {
    int aggIndex;
    int startIndex;
    int endIndex;
    long timerStart = DateTime.nanoTime();
    int jobs;
    boolean isBig;
    Predicate<GroupByFactory.GroupByJob> predicate;

    GB(int aggIndex, int startIndex, int endIndex) {
      this.aggIndex = aggIndex;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      predicate = aerospikeQueryNode.groupByFactory().predicate();
      isBig = predicate.test(this);
    }

    @Override
    public void run() {
      try {
        if (results.source().pipelineContext().queryContext().isClosed()) {
          return;
        }

        ++jobs;

        if (isBig) {
          ((BigSmallLinkedBlockingQueue) aerospikeQueryNode.groupByFactory().getQueue()).recordBigQueueWaitTime(timerStart);
        } else {
          ((BigSmallLinkedBlockingQueue) aerospikeQueryNode.groupByFactory().getQueue()).recordSmallQueueWaitTime(timerStart);
        }

        int end = Math.min(startIndex + aerospikeQueryNode.seriesPerJob(), endIndex);
        for (; startIndex < end; startIndex++) {
          final long hash = metaResult.getHash(startIndex);
          long start = System.nanoTime();
          LongTermStorage.Records records =
                  aerospikeQueryNode
                          .asClient()
                          .read(hash, aerospikeQueryNode.getSegmentsStart(), aerospikeQueryNode.getSegmentsEnd());
          long tt = System.nanoTime() - start;
          if (tt > maxASLatency) {
            maxASLatency = tt;
          }
          asLatencySum += tt;
          ++asLatencyCount;
          process(records, valuesCombiner[aggIndex]);
        }

        startIndex = end;
        if (startIndex < endIndex) {
          timerStart = DateTime.nanoTime();
          aerospikeQueryNode.groupByFactory().getQueue().put(this);
        } else {
          total_jobs.addAndGet(jobs);
          doneSignal.countDown();
        }
      } catch (Throwable t) {
        LOG.error("Failed to accumulate data", t);
        results.source().onError(new QueryDownstreamException(
                "Failed to execute group by job", t));
        doneSignal.countDown();
      }
    }

    @Override
    public int totalTsCount() {
      return totalTsCount;
    }
  }

  public void process() {
    try {
      LTSAerospike client = aerospikeQueryNode.asClient();

      if (jobCount > 1) {
        if (jobCount < aerospikeQueryNode.getGbThreads()) {
          for (int i = 0; i < aggrCount; i++) {
            final int startIndex = i * aerospikeQueryNode.seriesPerJob(); // inclusive
            final int endIndex; // exclusive
            if (i == jobCount - 1) {
              // last job
              endIndex = metaResult.numHashes();
            } else {
              endIndex = startIndex + aerospikeQueryNode.seriesPerJob();
            }

            GB job = new GB(i, startIndex, endIndex);
            aerospikeQueryNode.groupByFactory().getQueue().put(job);
          }
        } else {
          int startIndex = 0;
          int perJob = metaResult.numHashes() / aerospikeQueryNode.getGbThreads();
          for (int i = 0; i < aggrCount; i++) {
            final int endIndex;
            if (i + 1 == jobCount) {
              // last one
              endIndex = metaResult.numHashes();
            } else {
              endIndex = startIndex + perJob;
            }
            GB job = new GB(i, startIndex, endIndex);
            aerospikeQueryNode.groupByFactory().getQueue().put(job);
            startIndex += perJob;
          }
        }

        try {
          doneSignal.await();
        } catch (InterruptedException e) {
          LOG.error("GroupBy interrupted", e);
          return;
        }

        for (int x = 0; x < valuesCombiner.length; ++x) {
          combinedAggregator.combine(valuesCombiner[x]);

          try {
            valuesCombiner[x].close();
          } catch (IOException e) {
            LOG.error("Failed to close combiner, shouldn't happen", e);
          }
        }

        if (pooled_arrays != null) {
          for (int x = 0; x < pooled_arrays.length; ++x) {
            if (pooled_arrays[x] != null) {
              pooled_arrays[x].release();
            }
          }
        }
      } else {
        for (int i = 0; i < metaResult.numHashes(); i++) {
          if (results.source().pipelineContext().queryContext().isClosed()) {
            return;
          }

          final long hash = metaResult.getHash(i);
          long start = System.nanoTime();
          LongTermStorage.Records<BasicTimeSeriesEncoder> records =
                  aerospikeQueryNode
                          .asClient()
                          .read(hash, aerospikeQueryNode.getSegmentsStart(), aerospikeQueryNode.getSegmentsEnd());
          long tt = System.nanoTime() - start;
          if (tt > maxASLatency) {
            maxASLatency = tt;
          }
          asLatencySum += tt;
          ++asLatencyCount;
          process(records, combinedAggregator);
        }
      }

      if (combinedAggregator.end() <= combinedAggregator.offset()) {
        has_next = false;
      } else {
        has_next = true;
      }

      // done, log some stuff if we were told to
      if (queryTrace) {
        QueryContext ctx = results.source().pipelineContext().queryContext();
        // just to keep other GB's from writing at the same time.
        synchronized (ctx) {
          ctx.logTrace(results.source(), "Group ID [" + metaResult.id() + "] " + debugId);
          ctx.logTrace(results.source(), "Series in Group: " + metaResult.numHashes());
          ctx.logTrace(results.source(), "Threads Used: " + (jobCount > 1 ? aggrCount : 0));
          ctx.logTrace(results.source(), "Total Runs in Threads: " + (total_jobs != null ? total_jobs.get() : 0));
          ctx.logTrace(results.source(), "Max AS Latency: " + maxASLatency + "ns");
          ctx.logTrace(results.source(), "Avg AS Latency: " + ((double) asLatencySum / (double) asLatencyCount) + "ns");
          ctx.logTrace(results.source(), "Missing Segments: " + missingSegments);
          ctx.logTrace(results.source(), "Present Segments: " + validSegments);
          ctx.logTrace(results.source(), "Total Segments: " + validSegments + missingSegments);
          ctx.logTrace(results.source(), "Max Segment Decode Time: " + maxSegmentRead + "ns");
          ctx.logTrace(results.source(), "Avg Segment Decode Time: " + ((double) segmentReadSum / (double) segmentReadCount) + "ns");
        }
      }
    } catch (Throwable t) {
      LOG.error("Unexpected exception", t);
      has_next = false;
      aerospikeQueryNode.onError(t);
    }
  }

  protected NumericArrayAggregator createAggregator(
          final PooledObject[] pooled_arrays,
          final int index) {
    final NumericArrayAggregator aggregator = ((AerospikeQueryNode) results.source()).newAggregator();
    if (aggregator == null) {
      throw new IllegalArgumentException(
              "No aggregator found of type: "
                      + ((AerospikeQueryNode) results.source()).groupByConfig().getAggregator());
    }

    if (((AerospikeQueryNode) results.source()).doublePool() != null) {
      pooled_arrays[index] =
              ((AerospikeQueryNode) results.source())
                      .doublePool()
                      .claim(((AerospikeQueryNode) results.source()).downsampleConfig().intervals());
    }
    return aggregator;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
          final TypeToken<? extends TimeSeriesDataType> type) {
    if (type == NumericArrayType.TYPE) {
      return Optional.of(new TSIterator());
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> list = Lists.newArrayList();
    list.add(new TSIterator());
    return list;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    if (dsConfig != null) {
      return NumericArrayType.SINGLE_LIST;
    } else {
      return NumericType.SINGLE_LIST;
    }
  }

  @Override
  public void close() {
    //      node = null;
    //      results = null;
    //      id = null;
    //      cached_hash = 0;
    //      segmentTimes = null;
    //      if (id_byte_array_pool != null) {
    //        id_byte_array_pool.release();
    //        id_byte_array_pool = null;
    //      }
    release();
  }

  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    //      if (ts_pool != null) {
    //        ts_pool.release();
    //      }
  }

  @Override
  public void setPooledObject(PooledObject pooled_object) {
    //      this.ts_pool = pooled_object;

  }

  /**
   * Call this to actually load the data from AS and populate the group by array.
   * @param records The non-null records from AS.
   * @param aggregator The non-null GB aggregator.
   */
  void process(final LongTermStorage.Records<BasicTimeSeriesEncoder> records,
               final NumericArrayAggregator aggregator) {
    int previousRateTimestamp = -1;
    double previousRateValue = Double.NaN;
    double rateInterval = 0;
    long rateDataInterval = 0;
    int nonOptimizedIndex = 0;

    if (aerospikeQueryNode.rateConfig() != null) {
      rateInterval =
              ((double) DateTime.parseDuration(aerospikeQueryNode.rateConfig().getInterval()) /
                      (double) 1000);
      if (aerospikeQueryNode.rateConfig().getRateToCount() && aerospikeQueryNode.rateConfig().dataIntervalMs() > 0) {
        rateDataInterval = (aerospikeQueryNode.rateConfig().dataIntervalMs() / 1000) /
                aerospikeQueryNode.rateConfig().duration().get(ChronoUnit.SECONDS);
      }
    }

    final double[] values = aerospikeQueryNode.getSegmentReadArray();
    final double[] aggs = AerospikeQueryNode.DOWNSAMPLE_AGGREGATOR.get();
    TLongIntMap distribution = null;
    // run through the values.
    boolean computeDataInterval = false;
    if (aerospikeQueryNode.rateConfig() != null
            && aerospikeQueryNode.rateConfig().getRateToCount()
            && rateDataInterval < 1) {
      computeDataInterval = true;
      distribution = new TLongIntHashMap();
    }

    long start = System.currentTimeMillis();

    int intervalOffset = 0;
    boolean intervalHasValue = false;
    boolean intervalInfectedByNans = false;
    int intervalIndex = 0;

    int secondsInSegment = aerospikeQueryNode.getSecondsInSegment();
    int timeCounter = aerospikeQueryNode.getSegmentsStart();
    int segmentTime = 0;
    // NOTE This timer doesn't mean we're calling into AS each time but could just be
    // getting an entry from the map in in the buffer.
    long asStart = System.nanoTime();
    BasicTimeSeriesEncoder encoder = records.hasNext() ? records.next() : null;
    long tt = System.nanoTime() - asStart;
    if (tt > maxASLatency) {
      tt = maxASLatency;
    }
    asLatencySum += tt;
    ++asLatencyCount;

    if (encoder == null) {
      // no data for any segments.
      timeCounter = aerospikeQueryNode.getSegmentsEnd();
      missingSegments += (aerospikeQueryNode.getSegmentsEnd() - aerospikeQueryNode.getSegmentsStart()) / secondsInSegment;
      return;
    }

    while (timeCounter < aerospikeQueryNode.getSegmentsEnd()) {
      boolean segmentFound;
      if (encoder != null && encoder.getSegmentTime() == timeCounter) {
        segmentFound = true;
        segmentTime = encoder.getSegmentTime();
        ++validSegments;
      } else {
        if (encoder != null &&
                encoder.getSegmentTime() + secondsInSegment <= queryStartTime) {
          // this is something we need to debug so fail fast as it means we're likely
          // going to give the wrong results.
          throw new QueryDownstreamException("Found a segment at " + segmentTime
                  + " that shouldn't have been returned as our query start time is " + queryStartTime);
        }
        segmentFound = false;
        segmentTime = timeCounter;
        ++missingSegments;
      }
      timeCounter += secondsInSegment;

      int startIndex = 0;
      int stopIndex = secondsInSegment; // exclusive
      if (segmentTime < queryStartTime) {
        startIndex = queryStartTime - segmentTime;
      }

      if ((segmentTime + secondsInSegment) >= queryEndTime) {
        stopIndex = queryEndTime - segmentTime;
      }

      if (segmentFound) {
        // IMPORTANT! Reset accumulator for each segment and reuse
        Arrays.fill(values, Double.NaN);

        long readStart = System.nanoTime();
        int read = encoder.readAndDedupe(values);
        tt = System.nanoTime() - readStart;
        if (tt > maxSegmentRead) {
          maxSegmentRead = tt;
        }
        segmentReadSum += tt;
        ++segmentReadCount;
        if (read <= 0) {
          // shouldn't happen but...
          throw new QueryDownstreamException("[" + metaResult.id() + "] no data read from encoder?");
        }

        if (computeDataInterval) {
          int lastTimestamp = -1;
          for (int i = 0; i < values.length; i++) {
            if (Double.isNaN(values[i])) {
              continue;
            }

            if (lastTimestamp < 0) {
              lastTimestamp = segmentTime + i;
              continue;
            }

            int d = (segmentTime + i) - lastTimestamp;
            lastTimestamp = segmentTime + i;
            if (d > Integer.MIN_VALUE) {
              if (distribution.containsKey(d)) {
                distribution.increment(d);
              } else {
                distribution.put(d, 1);
              }
            }
          }
        }

        int t_segmentTime = segmentTime;
        int t_startIndex = startIndex;
        int t_stopIndex = stopIndex;
        int t_intervalIndex = intervalIndex;

        if (logTrace) {
          LOG.trace("Segment time: {}", segmentTime);
          LOG.trace("Start index: " + startIndex);
          LOG.trace("Stop index: " + stopIndex);
          LOG.trace("Interval index: " + intervalIndex);
        }

        try {
          if (computeDataInterval) {
            long diff = 0;
            int count = 0;
            final TLongIntIterator it = distribution.iterator();
            while (it.hasNext()) {
              it.advance();
              if (it.value() > count) {
                diff = it.key();
                count = it.value();
              }
            }
            rateDataInterval = diff / aerospikeQueryNode.rateConfig().duration().get(ChronoUnit.SECONDS);
            if (rateDataInterval < 1) {
              rateDataInterval = 1;
            }
          }
          for (; startIndex < stopIndex; startIndex++) {
            double value = values[startIndex];
            int timeStamp = segmentTime + startIndex;
            // skip those outside of the query range.
            if (timeStamp < queryStartTime) {
              continue;
            }
            if (timeStamp >= queryEndTime) {
              break;
            }

            if (aerospikeQueryNode.rateConfig() != null && !Double.isNaN(value)) {
              double rate;
              if (Double.isNaN(previousRateValue)) {
                rate = Double.NaN; // first rate is set to NaN
              } else {
                double dr = ((double) (timeStamp - previousRateTimestamp)) / rateInterval;
                if (aerospikeQueryNode.rateConfig().getRateToCount()) {
                  rate = value * (dr < rateDataInterval ? dr : rateDataInterval);
                } else if (aerospikeQueryNode.rateConfig().getDeltaOnly()) {
                  rate = value - previousRateValue;
                } else {
                  double valueDelta = value - previousRateValue;
                  if (aerospikeQueryNode.rateConfig().isCounter() && valueDelta < 0) {
                    if (aerospikeQueryNode.rateConfig().getDropResets()) {
                      rate = Double.NaN;
                    } else {
                      valueDelta =
                              aerospikeQueryNode.rateConfig().getCounterMax() - previousRateValue + value;
                      rate = valueDelta / dr;
                      if (aerospikeQueryNode.rateConfig().getResetValue()
                              > aerospikeQueryNode.rateConfig().DEFAULT_RESET_VALUE
                              && valueDelta > aerospikeQueryNode.rateConfig().getResetValue()) {
                        rate = Double.NaN;
                      }
                    }
                  } else {
                    rate = valueDelta / dr;
                  }
                }
              }
              previousRateTimestamp = timeStamp;
              previousRateValue = value;
              value = rate;
            }

            if (Double.isNaN(value)) {
//              if (infectiousNans) {
//                if (!intervalInfectedByNans) {
//                  intervalHasValue = false;
//                  intervalInfectedByNans = true;
//                }
//              }
            } else {
              if (/*!infectiousNans || */!intervalInfectedByNans) {
                aggs[0] += value; // sum
                aggs[1]++; // count
                if (value < aggs[2]) {
                  aggs[2] = value; // min
                }
                if (value > aggs[3]) {
                  aggs[3] = value; // max
                }
                aggs[4] = value; // last
                if (!intervalHasValue) {
                  intervalHasValue = true;
                }
              }

              if (dsAgg == AuraMetricsNumericArrayIterator.Agg.NON_OPTIMIZED) {
                if (nonOptimizedIndex + 1 >= nonOptimizedArray.length) {
                  double[] temp = new double[nonOptimizedArray.length * 2];
                  System.arraycopy(nonOptimizedArray, 0, temp, 0, nonOptimizedIndex);
                  nonOptimizedArray = temp;
                  if (nonOptimizedPooled != null) {
                    nonOptimizedPooled.release();
                  }
                }
                nonOptimizedArray[nonOptimizedIndex++] = value;
              }
            }
            intervalOffset++;

            if (intervalOffset == dsInterval) { // push it to group by
              if (intervalHasValue) {
                double v;
                switch (dsAgg) {
                  case SUM:
                    if (reportingAverage) {
                      v = aggs[0] / dsConfig.dpsInInterval();
                    } else {
                      v = aggs[0];
                    }
                    break;
                  case COUNT:
                    v = aggs[1];
                    break;
                  case MIN:
                    v = aggs[2];
                    break;
                  case MAX:
                    v = aggs[3];
                    break;
                  case LAST:
                    v = aggs[4];
                    break;
                  case AVG:
                    v = aggs[0] / aggs[1];
                    break;
                  case NON_OPTIMIZED:
                    nonOptimizedAggregator.run(
                            nonOptimizedArray,
                            0,
                            nonOptimizedIndex,
                            dsConfig.getInfectiousNan(),
                            nonOptimizedDp);
                    nonOptimizedIndex = 0;
                    v = nonOptimizedDp.toDouble();
                    break;
                  default:
                    throw new UnsupportedOperationException(
                            "Unsupported aggregator: " + aggregator);
                }
                if (logTrace) {
                  LOG.trace("Add to group by interval index: {}  value: {}", intervalIndex, v);
                }
                if (aggregator != null) {
                  aggregator.accumulate(v, intervalIndex++);
                } else {
                  throw new UnsupportedOperationException();
                  // nonGroupByResults[intervalIndex++] = v;
                }
              } else {
                if (logTrace) {
                  LOG.trace("Skip group by interval index: {}", intervalIndex);
                }
                intervalIndex++;
              }

              // reset interval
              for (int i = 0; i < aggs.length; i++) {
                aggs[i] = AuraMetricsNumericArrayIterator.IDENTITY[i];
              }
              intervalOffset = 0;
              intervalHasValue = false;
              intervalInfectedByNans = false;
            }
          }
        } catch (RuntimeException e) {
          LOG.error("Error during downsample", e);

          LOG.info("&&&&&&&&&&&&& Downsample in Aura &&&&&&&&&&&&&");
          LOG.info("Down sample start: " + dsConfig.getStart());
          LOG.info("Down sample end: " + dsConfig.getEnd());
          LOG.info("Down sample start time: " + queryStartTime);
          LOG.info("Down sample end time: " + queryEndTime);
          LOG.info("Down sample interval: " + dsInterval);
          LOG.info("Down sample intervals: " + dsConfig.intervals());
          // LOG.info("Segment Times: " + Arrays.toString(segmentTimes));
          LOG.info("Infectious NaN: " + "false"/*infectiousNans*/);
          LOG.info("&&&&&&&&&&&&&&&&&&&&&&");

          // LOG.info("Segment index: {} time: {}", t_segmentIndex, t_segmentTime);
          LOG.info("Start index: " + t_startIndex);
          LOG.info("Stop index: " + t_stopIndex);
          LOG.info("Interval index: " + t_intervalIndex);
          throw e;
        }

        // finished a segment so lets look to see if the query has timed out
        // before we possibly ask AS for another.
        if (results.source().pipelineContext().queryContext().isClosed()) {
          return;
        }

        // get the next segment
        if (records.hasNext()) {
          asStart = System.nanoTime();
          encoder = records.next();
          tt = System.nanoTime() - asStart;
          if (tt > maxASLatency) {
            tt = maxASLatency;
          }
          asLatencySum += tt;
          ++asLatencyCount;

          // we should not have out of order segments but... */
          if (encoder.getSegmentTime() <= segmentTime) {
            throw new QueryDownstreamException(
                    "Encountered an out of order segment " + encoder.getSegmentTime()
                            + " when the last time is " + segmentTime);
          }
        }
      } else {

        // check if intervals need to close or skip because of missing segment.
        int numIntervals = (intervalOffset + (stopIndex - startIndex)) / dsInterval;
        boolean intervalToBeClosed = numIntervals > 0;
        int intervalsToBeSkipped = intervalToBeClosed ? numIntervals - 1 : numIntervals;
        intervalOffset = (intervalOffset + (stopIndex - startIndex)) % dsInterval;

        if (intervalToBeClosed) {
          if (intervalHasValue) {
            // close the interval
            double v;
            switch (dsAgg) {
              case SUM:
                if (reportingAverage) {
                  v = aggs[0] / dsConfig.dpsInInterval();
                } else {
                  v = aggs[0];
                }
                break;
              case COUNT:
                v = aggs[1];
                break;
              case MIN:
                v = aggs[2];
                break;
              case MAX:
                v = aggs[3];
                break;
              case LAST:
                v = aggs[4];
                break;
              case AVG:
                v = aggs[0] / aggs[1];
                break;
              case NON_OPTIMIZED:
                nonOptimizedAggregator.run(
                        nonOptimizedArray,
                        0,
                        nonOptimizedIndex,
                        false/*, dsConfig.getInfectiousNan()*/,
                        nonOptimizedDp);
                nonOptimizedIndex = 0;
                v = nonOptimizedDp.toDouble();
                break;
              default:
                throw new UnsupportedOperationException(
                        "Unsupported aggregator: " + aggregator);
            }

            if (aggregator != null) {
              aggregator.accumulate(v, intervalIndex++); // push the interval to group by
            } else {
              throw new UnsupportedOperationException();
              // nonGroupByResults[intervalIndex++] = v;
            }
          } else {
            intervalIndex++;
          }
          // reset interval
          for (int i = 0; i < aggs.length; i++) {
            aggs[i] = AuraMetricsNumericArrayIterator.IDENTITY[i];
          }
          intervalHasValue = false;
          intervalInfectedByNans = false;
        }

        if (intervalsToBeSkipped > 0) {
          if (logTrace) {
            LOG.trace(
                    "Skip group by interval index from: {} to: {}",
                    intervalIndex,
                    (intervalIndex + intervalsToBeSkipped));
          }
          intervalIndex += intervalsToBeSkipped;
        }
      }
    }
  }

  class TSIterator
          implements TypedTimeSeriesIterator<NumericArrayType>, TimeSeriesValue<NumericArrayType> {

    boolean has_next = AerospikeGBTimeSeries.this.has_next;

    @Override
    public TypeToken<NumericArrayType> getType() {
      return NumericArrayType.TYPE;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public boolean hasNext() {
      if (!has_next) {
        return false;
      }
      has_next = false;
      return true;
    }

    @Override
    public TimeSeriesValue<NumericArrayType> next() {
      return this;
    }

    @Override
    public TimeStamp timestamp() {
      return aerospikeQueryNode.downsampleConfig().startTime();
    }

    @Override
    public NumericArrayType value() {
      return combinedAggregator;
    }

    @Override
    public TypeToken<NumericArrayType> type() {
      return NumericArrayType.TYPE;
    }
  }

  class TSID implements TimeSeriesStringId {

    Map<String, String> tags;

    @Override
    public String alias() {
      return null;
    }

    @Override
    public String namespace() {
      return null;
    }

    @Override
    public String metric() {
      return results.metricString();
    }

    @Override
    public Map<String, String> tags() {
      if (tags != null) {
        return tags;
      }

      tags = Maps.newHashMap();
      try {
        MetaTimeSeriesQueryResult.GroupResult.TagHashes hashes = metaResult.tagHashes();
        String[] tagKeys = ((AerospikeQueryNode) results.source()).gbKeys();
        if (tagKeys != null) {
          if (hashes.size() != tagKeys.length) {
            aerospikeQueryNode.onError(new QueryDownstreamException(
                    "Bad group by tags hash length. Got " + hashes.size()
                            + " when we need: " + tagKeys.length));
            return tags;
          }
          for (int i = 0; i < tagKeys.length; i++) {
            long hash = hashes.next();
            tags.put(tagKeys[i], results.metaResult().getStringForHash(hash));
          }
        }
      } catch (Throwable t) {
        aerospikeQueryNode.onError(t);
      }
      return tags;
    }

    @Override
    public String getTagValue(String s) {
      return tags().get(s);
    }

    @Override
    public List<String> aggregatedTags() {
      return Collections.emptyList();
    }

    @Override
    public List<String> disjointTags() {
      return Collections.emptyList();
    }

    @Override
    public Set<String> uniqueIds() {
      return null;
    }

    @Override
    public long hits() {
      return 0;
    }

    @Override
    public int compareTo(TimeSeriesStringId o) {
      return 0;
    }

    @Override
    public boolean encoded() {
      return false;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> type() {
      return Const.TS_STRING_ID;
    }

    @Override
    public long buildHashCode() {
      throw new UnsupportedOperationException();
    }
  }
}