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

package net.opentsdb.aura.metrics.storage;

import net.opentsdb.aura.metrics.core.RawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Locale;

public class AerospikeBatchGroupAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(AerospikeBatchGroupAggregator.class);

  protected AerospikeBatchQueryNode.QR results;
  protected MetaTimeSeriesQueryResult metaResult;
  protected AerospikeBatchQueryNode aerospikeQueryNode;

  protected boolean has_next;
  protected DownsampleConfig dsConfig;
  protected boolean infectiousNans;
  protected AuraMetricsNumericArrayIterator.Agg dsAgg;
  protected boolean reportingAverage;
  protected NumericAggregator nonOptimizedAggregator;
  protected PooledObject nonOptimizedPooled;
  protected double[] nonOptimizedArray;
  protected MutableNumericValue nonOptimizedDp;
  protected int queryStartTime;
  protected int queryEndTime;
  protected int dsInterval;
  protected TimeSeriesId id;
  protected NumericArrayAggregator combinedAggregator;

  protected long prevHash;
  protected int previousRateTimestamp = -1;
  protected double previousRateValue = Double.NaN;
  protected double rateInterval = 0;
  protected long rateDataInterval = 0;
  protected int nonOptimizedIndex = 0;
  protected PooledObject doublePool;
  protected double[] values;
  protected double[] aggs;
  protected TLongIntMap distribution = null;
  protected boolean computeDataInterval = false;
  protected int intervalOffset = 0;
  protected boolean intervalHasValue = false;
  protected boolean intervalInfectedByNans = false;
  protected int intervalIndex = 0;
  protected int lastRealSegmentTime;
  protected long gid;
  protected int gidx;
  protected boolean init;
  protected boolean hadError;

  public boolean isInit() {
    return init;
  }

  public void resetInit() {
    init = false;
  }

  public void reset(final long gid,
                    final int gidx,
                    final AerospikeBatchQueryNode.QR results,
                    final MetaTimeSeriesQueryResult metaResult) {
    init = true;
    this.gid = gid;
    this.gidx = gidx;
    this.results = results;
    this.metaResult = metaResult;
    aerospikeQueryNode = (AerospikeBatchQueryNode) results.source();
    if (values == null) {
      if (aerospikeQueryNode.getDoublePool() != null) {
        doublePool = aerospikeQueryNode.getDoublePool().claim(
                aerospikeQueryNode.getSecondsInSegment());
        if (doublePool != null) {
          values = (double[]) doublePool.object();
        } else {
          values = new double[aerospikeQueryNode.getSecondsInSegment()];
        }
      } else {
        values = new double[aerospikeQueryNode.getSecondsInSegment()];
      }
    }
    Arrays.fill(values, 0, aerospikeQueryNode.secondsInSegment, Double.NaN);
    dsConfig = aerospikeQueryNode.downsampleConfig();
    infectiousNans = dsConfig.getInfectiousNan();
    this.queryStartTime = (int) dsConfig.startTime().epoch();
    this.queryEndTime = (int) dsConfig.endTime().epoch();
    this.dsInterval =
            dsConfig.getRunAll()
                    ? (queryEndTime - queryStartTime)
                    : (int) dsConfig.interval().get(ChronoUnit.SECONDS);
    hadError = false;

    aggs = Arrays.copyOf(AerospikeQueryNode.IDENTITY, AerospikeQueryNode.IDENTITY.length);
    for (int i = 0; i < aggs.length; i++) {
      aggs[i] = AuraMetricsNumericArrayIterator.IDENTITY[i];
    }

    String dsAggString = dsConfig.getAggregator().toLowerCase(Locale.ROOT);
    if (dsAggString.equalsIgnoreCase("AVG") &&
            dsConfig.dpsInInterval() > 0) {
      reportingAverage = true;
      dsAggString = "sum";
    } else {
      reportingAverage = false;
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
        nonOptimizedAggregator = factory.newAggregator(infectiousNans);
        if (aerospikeQueryNode.getDoublePool() != null) {
          nonOptimizedPooled = aerospikeQueryNode.getDoublePool().claim(64);
          nonOptimizedArray = (double[]) nonOptimizedPooled.object();
        } else {
          nonOptimizedArray = new double[64];
        }
        nonOptimizedDp = new MutableNumericValue();
    }

    combinedAggregator = ((AerospikeBatchQueryNode) results.source()).newAggregator();

    if (aerospikeQueryNode.rateConfig() != null) {
      rateInterval =
              ((double) DateTime.parseDuration(aerospikeQueryNode.rateConfig().getInterval()) /
                      (double) 1000);
      if (aerospikeQueryNode.rateConfig().getRateToCount() && aerospikeQueryNode.rateConfig().dataIntervalMs() > 0) {
        rateDataInterval = (aerospikeQueryNode.rateConfig().dataIntervalMs() / 1000) /
                aerospikeQueryNode.rateConfig().duration().get(ChronoUnit.SECONDS);
      }
    }

    if (aerospikeQueryNode.rateConfig() != null
            && aerospikeQueryNode.rateConfig().getRateToCount()
            && rateDataInterval < 1) {
      computeDataInterval = true;
      distribution = new TLongIntHashMap();
    }
  }

  public void newSeries() {
    if (intervalHasValue) {
      // flush it!
      flushRemaining();
    }

    previousRateTimestamp = -1;
    previousRateValue = Double.NaN;
    rateInterval = 0;
    rateDataInterval = 0;
    nonOptimizedIndex = 0;
    distribution = null;
    computeDataInterval = false;
    intervalOffset = 0;
    intervalHasValue = false;
    intervalInfectedByNans = false;
    intervalIndex = 0;
    lastRealSegmentTime = 0;

    if (aerospikeQueryNode.rateConfig() != null) {
      rateInterval =
              ((double) DateTime.parseDuration(aerospikeQueryNode.rateConfig().getInterval()) /
                      (double) 1000);
      if (aerospikeQueryNode.rateConfig().getRateToCount() && aerospikeQueryNode.rateConfig().dataIntervalMs() > 0) {
        rateDataInterval = (aerospikeQueryNode.rateConfig().dataIntervalMs() / 1000) /
                aerospikeQueryNode.rateConfig().duration().get(ChronoUnit.SECONDS);
      }
    }

    if (aerospikeQueryNode.rateConfig() != null
            && aerospikeQueryNode.rateConfig().getRateToCount()
            && rateDataInterval < 1) {
      computeDataInterval = true;
      distribution = new TLongIntHashMap();
    }
  }

  public void addSegment(RawTimeSeriesEncoder encoder, long hash) {
    if (hadError) {
      return;
    }
    long start = System.currentTimeMillis();
    if (encoder == null) {
      LOG.error("WTF? Encoder was null... ");
      hadError = true;
      return;
    }

    int segmentTime = encoder.getSegmentTime();
    if (!(segmentTime + aerospikeQueryNode.getSecondsInSegment() >=
            queryStartTime && segmentTime < queryEndTime)) {
//      LOG.warn("Segment was outside of the query time range??? " +
//                      "SegmentTime {}, queryStart {}, queryEnd {}",
//              segmentTime, queryStartTime, queryEndTime);
    }
//    LOG.info("PROCESSING records. Segment time {} QS: {}  QE {}",
//            segmentTime, queryStartTime, queryEndTime);

    int startIndex = 0;
    int stopIndex = aerospikeQueryNode.getSecondsInSegment(); // exclusive
    //LOG.info("INIT STOP IDX: " + stopIndex);

    if (segmentTime < queryStartTime) {
      startIndex = queryStartTime - segmentTime;
    }
    if ((segmentTime + aerospikeQueryNode.getSecondsInSegment()) >= queryEndTime) {
//      LOG.info("Seg past end: " + (segmentTime + aerospikeQueryNode.getSecondsInSegment()) + " Delta: " +
//              ((segmentTime + aerospikeQueryNode.getSecondsInSegment()) - queryEndTime));
      stopIndex = queryEndTime - segmentTime;
    }

    if (startIndex >= stopIndex) {
      // ignore segments out side of query range
      //LOG.error("Segment outside of range? " + startIndex + " END: " + stopIndex);
      return;
    }

    if (prevHash != hash) {
      newSeries();
      prevHash = hash;
    }

    // see if we need to fill
    int newIndex = startInterval(segmentTime);
    if (newIndex < intervalIndex) {
      // TODO - set an exception flag.
      aerospikeQueryNode.onError(new QueryDownstreamException(
              "Encountered an out of order segment " + encoder.getSegmentTime()
                      + " when the last time is " + segmentTime));
      hadError = true;
      return;
    }
    if (newIndex > intervalIndex + 1) {
      // could equal or be the next and be ok. BUT if it's more than that, we
      // need to fill potentially.
      if (intervalHasValue) {
        // flush it!
        flushRemaining();
      }
    }

    intervalIndex = newIndex;

    lastRealSegmentTime = segmentTime;
    // Reset accumulator for each segment and reuse
    Arrays.fill(values, 0, aerospikeQueryNode.secondsInSegment, Double.NaN);

    int read = encoder.readAndDedupe(values);
    if (read <= 0) {
      // shouldn't happen but...
      //LOG.info("[" + groupResult.id() + "] => WTF no data from encoder?");
      hadError = true;
      return;
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

    if (LOG.isTraceEnabled()) {
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
//          if (infectiousNans) {
//            if (!intervalInfectedByNans) {
//              intervalHasValue = false;
//              intervalInfectedByNans = true;
//            }
//          }
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
            final double v;
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
                        "Unsupported aggregator: " + combinedAggregator);
            }
            combinedAggregator.accumulate(v, intervalIndex++);
          } else {
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
      LOG.info("Infectious NaN: " + infectiousNans);
      LOG.info("&&&&&&&&&&&&&&&&&&&&&&");

      // LOG.info("Segment index: {} time: {}", t_segmentIndex, t_segmentTime);
      LOG.info("Start index: " + t_startIndex);
      LOG.info("Stop index: " + t_stopIndex);
      LOG.info("Interval index: " + t_intervalIndex);
      throw e;
    }
  }

  public NumericArrayAggregator flush() {
    if (hadError) {
      return null;
    }

    if (intervalHasValue) {
      flushRemaining();
    }

    if (doublePool != null) {
      doublePool.release();
      doublePool = null;
      values = null;
    }

    if (nonOptimizedPooled != null) {
      nonOptimizedPooled.release();
      nonOptimizedPooled = null;
      nonOptimizedArray = null;
    }
    return combinedAggregator;
  }

  public long gid() {
    return gid;
  }

  int startInterval(final int segmentTime) {
    if (segmentTime <= queryStartTime) {
      // assume we had a check before this to make sure the segment is valid
      return 0;
    }
    return (segmentTime - queryStartTime) / dsInterval;
  }

  void flushRemaining() {
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
                "Unsupported aggregator: " + combinedAggregator);
    }
    combinedAggregator.accumulate(v, intervalIndex++); // push the interval to group by

    // reset interval
    for (int i = 0; i < aggs.length; i++) {
      aggs[i] = AuraMetricsNumericArrayIterator.IDENTITY[i];
    }
    intervalHasValue = false;
    intervalInfectedByNans = false;
  }

  public int gidx() {
    return gidx;
  }

}