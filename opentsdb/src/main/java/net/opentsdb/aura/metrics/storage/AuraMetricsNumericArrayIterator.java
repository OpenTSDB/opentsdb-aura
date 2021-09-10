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

import com.google.common.reflect.TypeToken;
import net.opentsdb.aura.metrics.core.RawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoderFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import net.opentsdb.data.AggregatingTypedTimeSeriesIterator;
import net.opentsdb.data.Aggregator;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;

public class AuraMetricsNumericArrayIterator implements
        AggregatingTypedTimeSeriesIterator<NumericArrayType>,
        TimeSeriesValue<NumericArrayType>,
        NumericArrayType {

  private static final Logger logger = LoggerFactory.getLogger(AuraMetricsNumericArrayIterator.class);
  public static ThreadLocal<Accumulator> threadLocalAccs;
  public static final double[] IDENTITY = {0.0, 0.0, Double.MAX_VALUE, -Double.MAX_VALUE, 0.0};

  //TODO: inject the factories
  public static TimeSeriesEncoderFactory<? extends RawTimeSeriesEncoder> timeSeriesEncoderFactory;
  public static TimeSeriesRecordFactory timeSeriesRecordFactory;

  /**
   * holds the intermediate aggregations for an interval.
   * 1. sum
   * 2. count
   * 3. min
   * 4. max
   * 5. last
   */
  public static final ThreadLocal<double[]> threadLocalAggs = ThreadLocal.withInitial(() -> Arrays.copyOf(IDENTITY, IDENTITY.length));
  static final ThreadLocal<TimeSeriesRecord> threadLocalTimeseries = ThreadLocal.withInitial(() -> timeSeriesRecordFactory.create());
  static final ThreadLocal<RawTimeSeriesEncoder> threadTSEncoder = ThreadLocal.withInitial(() -> timeSeriesEncoderFactory.create());

  public enum Agg {
    SUM,
    AVG,
    COUNT,
    MIN,
    MAX,
    LAST,
    NON_OPTIMIZED
  }

  private final QueryNode node;
  private final long tsPointer;
  private final DownsampleConfig downsampleConfig;
  private final RateConfig rateConfig;

  private boolean was_read;

  private NumericArrayAggregator numericArrayAggregator;
  private NumericAggregator nonOptimizedAggregator;
  private PooledObject nonOptimizedPooled;
  private double[] nonOptimizedArray;
  private int nonOptimizedIndex;
  private MutableNumericValue nonOptimizedDp;
  private PooledObject nonGroupByPooled;
  private double[] nonGroupByResults;
  private int intervalIndex;

  private final int startTime;
  private final int endTime;
  private final int interval;
  private final Agg aggregator;
  private final boolean infectiousNans;
  private final int secondsInSegment;
  private int firstSegmentTime;
  private int segmentCount;

  private final StatsCollector statsCollector;

  /** Whether or not we're using an expected reporting interval. */
  protected final boolean reporting_average;

  private int previousRateTimestamp = -1;
  private double previousRateValue = Double.NaN;
  private double rateInterval;
  private long rateDataInterval;

  public AuraMetricsNumericArrayIterator(
          final DownsampleConfig ds_config,
          final RateConfig rate_config,
          final QueryNode node,
          final AuraMetricsQueryResult results,
          final long tsPointer,
          final int firstSegmentTime,
          final int segmentCount) {
    this.node = node;
    this.tsPointer = tsPointer;
    this.downsampleConfig = ds_config;
    this.rateConfig = rate_config;
    this.statsCollector = node.pipelineContext().tsdb().getStatsCollector();
    TimeSeriesStorageIf timeSeriesStorage = ((AuraMetricsSourceFactory) ((AbstractQueryNode) node).factory()).timeSeriesStorage();
    secondsInSegment = timeSeriesStorage.secondsInSegment();

    // TODO - gag!!
    if (threadLocalAccs == null) {
      synchronized (AuraMetricsNumericIterator.class) {
        if (threadLocalAccs == null) {
          threadLocalAccs = ThreadLocal.withInitial(() -> new Accumulator(secondsInSegment));
        }
      }
    }

    String agg = downsampleConfig.getAggregator().toLowerCase();
    if (agg.equalsIgnoreCase("AVG") && downsampleConfig.dpsInInterval() > 0) {
      reporting_average = true;
      agg = "sum";
    } else {
      reporting_average = false;
    }

    switch (agg) {
      case "sum":
      case "zimsum":
        aggregator = Agg.SUM;
        break;
      case "avg":
        aggregator = Agg.AVG;
        break;
      case "count":
        aggregator = Agg.COUNT;
        break;
      case "max":
      case "mimmax":
        aggregator = Agg.MAX;
        break;
      case "min":
      case "mimmin":
        aggregator = Agg.MIN;
        break;
      case "last":
        aggregator = Agg.LAST;
        break;
      default:
        aggregator = Agg.NON_OPTIMIZED;
        final NumericAggregatorFactory factory = node.pipelineContext().tsdb().getRegistry()
                .getPlugin(NumericAggregatorFactory.class, agg);
        nonOptimizedAggregator = factory.newAggregator(downsampleConfig.getInfectiousNan());
        ObjectPool pool = node.pipelineContext().tsdb().getRegistry()
                .getObjectPool(DoubleArrayPool.TYPE);
        if (pool != null) {
          nonOptimizedPooled = ((ArrayObjectPool) pool).claim(64);
          nonOptimizedArray = (double[]) nonOptimizedPooled.object();
        } else {
          nonOptimizedArray = new double[64];
        }
        nonOptimizedDp = new MutableNumericValue();
    }

    this.startTime = (int) downsampleConfig.startTime().epoch();
    this.endTime = (int) downsampleConfig.endTime().epoch();
    this.interval =
            downsampleConfig.getRunAll()
                    ? (endTime - startTime)
                    : (int) downsampleConfig.interval().get(ChronoUnit.SECONDS);

    this.infectiousNans = downsampleConfig.getInfectiousNan();

    if (aggregator != Agg.NON_OPTIMIZED) {
      final NumericArrayAggregatorFactory factory =
              node.pipelineContext()
                      .tsdb()
                      .getRegistry()
                      .getPlugin(NumericArrayAggregatorFactory.class, agg);
      if (factory == null) {
        throw new IllegalArgumentException(
                "No numeric array aggregator factory found for type: " + agg);
      }
      numericArrayAggregator = (NumericArrayAggregator) factory.newAggregator(
              results.aggregatorConfig());
    } else {
      numericArrayAggregator = null;
    }

    if (rateConfig != null) {
      rateInterval =
              ((double) DateTime.parseDuration(rateConfig.getInterval()) /
                      (double) 1000);
      if (rateConfig.getRateToCount() && rateConfig.dataIntervalMs() > 0) {
        rateDataInterval = (rateConfig.dataIntervalMs() / 1000) /
                rateConfig.duration().get(ChronoUnit.SECONDS);
      }
    }

    this.firstSegmentTime = firstSegmentTime;
    this.segmentCount = segmentCount;

    if(logger.isTraceEnabled()) {
      logger.trace("&&&&&&&&&&&&& Downsample in Aura &&&&&&&&&&&&&");
      logger.trace("Down sample start: {}", ds_config.getStart());
      logger.trace("Down sample end: {}", ds_config.getEnd());
      logger.trace("Down sample start time: {}", startTime);
      logger.trace("Down sample end time: {}", endTime);
      logger.trace("Down sample interval: {}", interval);
      logger.trace("Down sample intervals: {}", ds_config.intervals());
      logger.trace("First segment time: {}", firstSegmentTime);
      logger.trace("Infectious NaN: {}" + infectiousNans);
      logger.trace("&&&&&&&&&&&&&&&&&&&&&&");
    }
  }

  @Override
  public TimeStamp timestamp() {
    return downsampleConfig.startTime();
  }

  @Override
  public NumericArrayType value() {
    return this;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public boolean hasNext() {
    return !was_read;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    ObjectPool pool = node.pipelineContext().tsdb().getRegistry()
            .getObjectPool(DoubleArrayPool.TYPE);
    if (pool != null) {
      nonGroupByPooled = ((ArrayObjectPool) pool).claim(downsampleConfig.intervals());
      nonGroupByResults = (double[]) nonGroupByPooled.object();
    } else {
      nonGroupByResults = new double[downsampleConfig.intervals()];
    }
    Arrays.fill(nonGroupByResults, Double.NaN);
    next(null);
    return this;
  }

  @Override
  public void next(final Aggregator aggregator) {
    numericArrayAggregator = (aggregator != null ? (NumericArrayAggregator) aggregator :
            null); // group by aggregator
    was_read = true;
    final Accumulator tlAccumulator = threadLocalAccs.get();
    final double[] aggs = threadLocalAggs.get();
    TLongIntMap distribution = null;
    // run through the values.
    boolean computeDataInterval = false;
    if (rateConfig != null && rateConfig.getRateToCount() && rateDataInterval < 1) {
      computeDataInterval = true;
      distribution = new TLongIntHashMap();
    }

    long start = System.currentTimeMillis();

    TimeSeriesRecord timeSeriesRecord = threadLocalTimeseries.get();
    timeSeriesRecord.open(tsPointer);

    int intervalOffset = 0;
    boolean intervalHasValue = false;
    boolean intervalInfectedByNans = false;

    RawTimeSeriesEncoder timeSeriesEncoder = threadTSEncoder.get();
    TimeSeriesStorageIf timeSeriesStorage = ((AuraMetricsSourceFactory) node.factory()).timeSeriesStorage();
    int secondsInSegment = timeSeriesStorage.secondsInSegment();

    int shiftSeconds = ((AuraMetricsQueryNode) node).shiftSeconds();
    int segmentTime = firstSegmentTime;
    int segmentIndex = 0;
    while(segmentIndex < segmentCount) {
      if (segmentTime == firstSegmentTime && startTime - shiftSeconds < segmentTime) {
        // skipIntervals
        int intervalsToSkip = (segmentTime - (startTime - shiftSeconds)) / interval;
        intervalIndex += intervalsToSkip;
      }

      int startIndex = 0;
      int stopIndex = secondsInSegment; // exclusive

      if (segmentTime < startTime - shiftSeconds) {
        startIndex = (startTime - shiftSeconds) - segmentTime;
      }
      if ((segmentTime + secondsInSegment) >= endTime - shiftSeconds) {
        stopIndex = (endTime - shiftSeconds) - segmentTime;
      }

      if (startIndex >= stopIndex) {
        segmentTime += secondsInSegment;
        // ignore segments out side of query range
        continue;
      }

      boolean segmentFound = false;
      long segmentAddress = timeSeriesRecord.getSegmentAddress(segmentTime);
      if (segmentAddress != 0) {
        timeSeriesEncoder.openSegment(segmentAddress);
        if (segmentTime == timeSeriesEncoder.getSegmentTime()) {
          segmentFound = true;
        } else {
          if(logger.isDebugEnabled()) {
            logger.debug(
                    "Discard stale segment, query time: {}, segment time: {}",
                    segmentTime,
                    timeSeriesEncoder.getSegmentTime());
          }
        }
      }

      if (segmentFound) {

        StatsCollector.StatsTimer segmentDecodeTimer = statsCollector.startTimer("segment.decode.time", ChronoUnit.NANOS);

        // Reset accumulator for each segment and reuse
        tlAccumulator.reset();
        tlAccumulator.baseTimeStamp = segmentTime;

        timeSeriesEncoder.readAndDedupe(tlAccumulator.values);

        if(computeDataInterval) {

          int lastTimestamp = -1;
          for (int i = 0; i < tlAccumulator.length; i++) {
            if (Double.isNaN(tlAccumulator.values[i])) {
              continue;
            }

            if (lastTimestamp < 0) {
              lastTimestamp = segmentTime + i;
              continue;
            }

            int d = (segmentTime + i) - lastTimestamp;
            lastTimestamp = segmentTime + i;
            if(d > Integer.MIN_VALUE) {
              if (distribution.containsKey(d)) {
                distribution.increment(d);
              } else {
                distribution.put(d, 1);
              }
            }
          }
        }

        double[] values = tlAccumulator.values;

        int t_segmentIndex = segmentIndex;
        int t_segmentTime = segmentTime;
        int t_startIndex = startIndex;
        int t_stopIndex = stopIndex;
        int t_intervalIndex = intervalIndex;

        if (logger.isTraceEnabled()) {
          logger.trace("Segment index: {} time: {}", segmentIndex, segmentTime);
          logger.trace("Start index: " + startIndex);
          logger.trace("Stop index: " + stopIndex);
          logger.trace("Interval index: " + intervalIndex);
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
            rateDataInterval = diff / rateConfig.duration().get(ChronoUnit.SECONDS);
            if (rateDataInterval < 1) {
              rateDataInterval = 1;
            }
          }
          for (; startIndex < stopIndex; startIndex++) {
            double value = values[startIndex];
            int timeStamp = segmentTime + shiftSeconds + startIndex;
            // skip those outside of the query range.
            if (timeStamp < startTime) {
              continue;
            }
            if (timeStamp >= endTime) {
              break;
            }

            if (rateConfig != null && !Double.isNaN(value)) {
              double rate;
              if (Double.isNaN(previousRateValue)) {
                rate = Double.NaN; // first rate is set to NaN
              } else {
                double dr = ((double) (timeStamp - this.previousRateTimestamp)) / rateInterval;
                if (rateConfig.getRateToCount()) {
                  rate = value * (dr < rateDataInterval ? dr : rateDataInterval);
                } else if (rateConfig.getDeltaOnly()) {
                  rate = value - previousRateValue;
                } else {
                  double valueDelta = value - previousRateValue;
                  if (rateConfig.isCounter() && valueDelta < 0) {
                    if (rateConfig.getDropResets()) {
                      rate = Double.NaN;
                    } else {
                      valueDelta = rateConfig.getCounterMax() - previousRateValue + value;
                      rate = valueDelta / dr;
                      if (rateConfig.getResetValue() > RateConfig.DEFAULT_RESET_VALUE
                              && valueDelta > rateConfig.getResetValue()) {
                        rate = 0D;
                      }
                    }
                  } else {
                    rate = valueDelta / dr;
                  }
                }
              }
              this.previousRateTimestamp = timeStamp;
              this.previousRateValue = value;
              value = rate;
            }

            if (Double.isNaN(value)) {
              if (infectiousNans) {
                if (!intervalInfectedByNans) {
                  intervalHasValue = false;
                  intervalInfectedByNans = true;
                }
              }
            } else {
              if (!infectiousNans || !intervalInfectedByNans) {
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

              if (this.aggregator == Agg.NON_OPTIMIZED) {
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

            if (intervalOffset == interval) { // push it to group by
              if (intervalHasValue) {
                double v;
                switch (this.aggregator) {
                  case SUM:
                    if (reporting_average) {
                      v = aggs[0] / downsampleConfig.dpsInInterval();
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
                    nonOptimizedAggregator.run(nonOptimizedArray, 0, nonOptimizedIndex, downsampleConfig.getInfectiousNan(), nonOptimizedDp);
                    nonOptimizedIndex = 0;
                    v = nonOptimizedDp.toDouble();
                    break;
                  default:
                    throw new UnsupportedOperationException(
                            "Unsupported aggregator: " + this.aggregator);
                }
                if(logger.isTraceEnabled()) {
                  logger.trace("Add to group by interval index: {}  value: {}", intervalIndex, v);
                }
                if (numericArrayAggregator != null) {
                  numericArrayAggregator.accumulate(v, intervalIndex++);
                } else {
                  nonGroupByResults[intervalIndex++] = v;
                }
              } else {
                if(logger.isTraceEnabled()) {
                  logger.trace("Skip group by interval index: {}", intervalIndex);
                }
                intervalIndex++;
              }

              // reset interval
              for (int i = 0; i < aggs.length; i++) {
                aggs[i] = IDENTITY[i];
              }
              intervalOffset = 0;
              intervalHasValue = false;
              intervalInfectedByNans = false;
            }
          }
        } catch (RuntimeException e) {
          logger.error("Error during downsample", e);

          logger.info("&&&&&&&&&&&&& Downsample in Aura &&&&&&&&&&&&&");
          logger.info("Down sample start: " + downsampleConfig.getStart());
          logger.info("Down sample end: " + downsampleConfig.getEnd());
          logger.info("Down sample start time: " + startTime);
          logger.info("Down sample end time: " + endTime);
          logger.info("Down sample interval: " + interval);
          logger.info("Down sample intervals: " + downsampleConfig.intervals());
          logger.info("First Segment time: " + firstSegmentTime);
          logger.info("Segment count: " + segmentCount);
          logger.info("Infectious NaN: " + infectiousNans);
          logger.info("&&&&&&&&&&&&&&&&&&&&&&");

          logger.info("Segment index: {} time: {}", t_segmentIndex, t_segmentTime);
          logger.info("Start index: " + t_startIndex);
          logger.info("Stop index: " + t_stopIndex);
          logger.info("Interval index: " + t_intervalIndex);
          throw e;
        }
        segmentDecodeTimer.stop();
      } else {

        // check if intervals need to close or skip because of missing segment.
        int numIntervals = (intervalOffset + (stopIndex - startIndex)) / interval;
        boolean intervalToBeClosed = numIntervals > 0;
        int intervalsToBeSkipped = intervalToBeClosed ? numIntervals - 1 : numIntervals;
        intervalOffset = (intervalOffset + (stopIndex - startIndex)) % interval;

        if (intervalToBeClosed) {
          if (intervalHasValue) {
            // close the interval
            double v;
            switch (this.aggregator) {
              case SUM:
                if (reporting_average) {
                  v = aggs[0] / downsampleConfig.dpsInInterval();
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
                nonOptimizedAggregator.run(nonOptimizedArray, 0, nonOptimizedIndex, downsampleConfig.getInfectiousNan(), nonOptimizedDp);
                nonOptimizedIndex = 0;
                v = nonOptimizedDp.toDouble();
                break;
              default:
                throw new UnsupportedOperationException(
                        "Unsupported aggregator: " + this.aggregator);
            }
            if(logger.isTraceEnabled()) {
              logger.trace("Add to group by interval index: {}  value: {}", intervalIndex, v);
            }
            if (numericArrayAggregator != null) {
              numericArrayAggregator.accumulate(v, intervalIndex++); // push the interval to group by
            } else {
              nonGroupByResults[intervalIndex++] = v;
            }
          } else {
            if(logger.isTraceEnabled()) {
              logger.trace("Skip group by interval index: {}", intervalIndex);
            }
            intervalIndex++;
          }
          // reset interval
          for (int i = 0; i < aggs.length; i++) {
            aggs[i] = IDENTITY[i];
          }
          intervalHasValue = false;
          intervalInfectedByNans = false;
        }

        if (intervalsToBeSkipped > 0) {
          if (logger.isTraceEnabled()) {
            logger.trace(
                    "Skip group by interval index from: {} to: {}",
                    intervalIndex,
                    (intervalIndex + intervalsToBeSkipped));
          }
          intervalIndex += intervalsToBeSkipped;
        }
      }
      segmentIndex++;
      segmentTime += secondsInSegment;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
              "Total downsample time for {} segments is {} ms",
              segmentCount,
              System.currentTimeMillis() - start);
    }
    return;
  }

  @Override
  public TypeToken<NumericArrayType> getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long[] longArray() {
    return null;
  }

  @Override
  public double[] doubleArray() {
    return numericArrayAggregator == null ? nonGroupByResults :
            numericArrayAggregator.doubleArray();
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return numericArrayAggregator == null ? intervalIndex : numericArrayAggregator.end();
  }

  @Override
  public void close() {
    if (nonOptimizedPooled != null) {
      nonOptimizedPooled.release();
    }
    if (nonGroupByPooled != null) {
      nonGroupByPooled.release();
    }
  }

  private void storeValue(Accumulator accumulator, int timestamp, double value) {
    // skip those outside of the query range.
    if (timestamp < startTime || timestamp >= endTime) {
      return;
    }
    accumulator.add(timestamp, value);
  }

  public static class Accumulator {
    public int baseTimeStamp;
    public double[] values;
    public int length;
//    int size;

    public Accumulator(int length) {
      this.values = new double[length];
      this.length = length;
    }

    public void add(int timestamp, double value) {

      int index = timestamp % length;
//      if (Double.isNaN(values[index])) {
//        size++;
//      }
      values[index] = value;
    }

    public void reset() {
      Arrays.fill(values, Double.NaN);
//      size = 0;
    }

  }
}