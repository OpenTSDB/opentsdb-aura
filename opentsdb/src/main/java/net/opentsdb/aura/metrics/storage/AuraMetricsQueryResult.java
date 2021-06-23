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
import net.opentsdb.common.Const;
import net.opentsdb.data.ArrayAggregatorConfig;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.query.*;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.List;


public class AuraMetricsQueryResult implements QueryResult, TimeSpecification {

    private static final Logger logger = LoggerFactory.getLogger(AuraMetricsQueryResult.class);

    protected final QueryNode queryNode;

    private AuraMetricsTimeSeriesList timeseries;
    private long[] resultPointers;
    private int firstSegmentTime;
    private int segmentCount;
    //protected Map<byte[], DP> tsLastMap;

    protected DownsampleConfig downsampleConfig;
    protected RateConfig rateConfig;
    protected ArrayAggregatorConfig aggregatorConfig;
    private byte[] metric; // set by TimeSeriesStorage.query.
    
    protected long dataParseNanos;
    protected long dataSortNanos;
    protected long dataDedupeNanos;
    protected long dataAggNanos;
    protected long dataStoreNanos;
    protected long dataStoreNanos0;
    protected long dataStoreNanos1;
    protected long dataStoreNanos2;
    protected long dataStoreInc;
    protected long dataStoreDec;
    private RollupConfig rollupConfig;

    public AuraMetricsQueryResult(QueryNode queryNode, RollupConfig rollupConfig) {
        this.rollupConfig = rollupConfig;
        this.queryNode = queryNode;
        final List<QueryNodeConfig> nodes =
            ((TimeSeriesDataSourceConfig) queryNode.config()).getPushDownNodes();
        if (nodes != null) {
          for (final QueryNodeConfig node : nodes) {
            if (node != null && node instanceof DownsampleConfig) {
              downsampleConfig = (DownsampleConfig) node;
              if (((TimeSeriesDataSourceConfig) queryNode.config()).timeShifts() != null) {
                downsampleConfig.startTime().subtract((TemporalAmount) ((TimeSeriesDataSourceConfig)
                    queryNode.config()).timeShifts().getValue());
                downsampleConfig.endTime().subtract((TemporalAmount) ((TimeSeriesDataSourceConfig)
                    queryNode.config()).timeShifts().getValue());
              }
              aggregatorConfig = DefaultArrayAggregatorConfig.newBuilder()
                  .setArraySize(downsampleConfig.intervals())
                  .setInfectiousNaN(downsampleConfig.getInfectiousNan())
                  .build();
            } else if (node != null && node instanceof RateConfig) {
              rateConfig = (RateConfig) node;
            }
          }
        }

//        if (((TimeSeriesDataSourceConfig) queryNode.config()).getFetchLast()) {
//          tsLastMap = new ConcurrentHashMap<>();
//        }

        metric =  ((TimeSeriesDataSourceConfig) queryNode.config())
            .getMetric().getMetric().getBytes(Const.UTF8_CHARSET);
        resultPointers = new long[
                ((AuraMetricsSourceFactory) ((AbstractQueryNode) queryNode).factory()).timeSeriesStorage().numShards()];
    }

    @Override
    public TimeSpecification timeSpecification() {
        return downsampleConfig != null ? this : null;
    }

    @Override
    public List<TimeSeries> timeSeries() {
      return timeseries;
    }

    @Override
    public String error() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Throwable exception() {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public long sequenceId() {
        return 0;
    }

    @Override
    public QueryNode source() {
        return queryNode;
    }

    @Override
    public QueryResultId dataSource() {
      return (QueryResultId) queryNode.config().resultIds().get(0);
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
        return Const.TS_BYTE_ID;
    }

    @Override
    public ChronoUnit resolution() {
        return ChronoUnit.SECONDS;
    }

    @Override
    public RollupConfig rollupConfig() {
        return rollupConfig;
    }

    @Override
    public void close() {
      final Span span = queryNode.pipelineContext().queryContext().stats().querySpan();
      if (span != null) {
        span.setTag("auradb.totalParseTime.nanos", this.dataParseNanos);
        span.setTag("auradb.totalAggTime.nanos", this.dataAggNanos);
        span.setTag("auradb.totalSortTime.nanos", this.dataSortNanos);
        span.setTag("auradb.totalDedupeTime.nanos", this.dataDedupeNanos);
        span.finish();
      }
      if (timeseries != null) {
        timeseries.close();
      }
      timeseries = null;
      //tsLastMap = null;
    }

    @Override
    public boolean processInParallel() {
      return true;
    }

    public void addSegmentTimes(final int firstSegmentTime, final int segmentCount) {
      this.firstSegmentTime = firstSegmentTime;
      this.segmentCount = segmentCount;
    }

    public void addAllTimeSeries(final int shardId, final long tsAndTagPointer) {
      resultPointers[shardId] = tsAndTagPointer;
    }

//    public synchronized void addDP(final byte[] tags, final DP dp) {
//        tsLastMap.put(tags, dp);
//    }
    
    @Override
    public TimeStamp start() {
      return downsampleConfig.startTime();
    }

    @Override
    public TimeStamp end() {
      return downsampleConfig.endTime();
    }

    @Override
    public TemporalAmount interval() {
      return downsampleConfig.interval();
    }

    @Override
    public String stringInterval() {
      return downsampleConfig.getInterval();
    }

    @Override
    public ChronoUnit units() {
      return downsampleConfig.units();
    }

    @Override
    public ZoneId timezone() {
      return downsampleConfig.timezone();
    }

    @Override
    public void updateTimestamp(int offset, TimeStamp timestamp) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException();
    }

    @Override
    public void nextTimestamp(TimeStamp timestamp) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException();
    }
    
    public byte[] metric() {
      return metric;
    }

    void finished() {
      //TODO: add instrumentation
      logger.info("HAD " + resultPointers.length + " pointers.");
      timeseries = new AuraMetricsTimeSeriesList(this, resultPointers, firstSegmentTime, segmentCount);
      logger.info("Time Series count {}", timeseries.size());
    }

    DownsampleConfig downsampleConfig() {
      return downsampleConfig;
    }

    RateConfig rateConfig() {
      return rateConfig;
    }

    ArrayAggregatorConfig aggregatorConfig() {
      return aggregatorConfig;
    }
}
