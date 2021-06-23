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

import com.google.common.base.Strings;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.core.TimeseriesStorageContext;
import net.opentsdb.aura.metrics.meta.MetricQuery;
import net.opentsdb.aura.metrics.meta.Query;
import net.opentsdb.aura.metrics.QueryBuilderTSDBExt;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.TemporalAmount;

public class AuraMetricsQueryNode extends AbstractQueryNode implements TimeSeriesDataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuraMetricsQueryNode.class);

  private TimeSeriesDataSourceConfig config;
  private RollupConfig rollupConfig;
  private TimeSeriesStorageIf timeSeriesStorage;
  private TimeseriesStorageContext storageContext;
  private TimeSeriesRecord timeSeriesRecord;
  private boolean queryIncludesNamespace;

  public AuraMetricsQueryNode(
      final AuraMetricsSourceFactory factory,
      final QueryPipelineContext context,
      final TimeSeriesDataSourceConfig config,
      final RollupConfig rollupConfig,
      final TimeSeriesStorageIf timeSeriesStorage,
      final TimeSeriesRecord timeSeriesRecord, final boolean queryIncludesNamespace) {
    super(factory, context);
    this.config = config;
    this.rollupConfig = rollupConfig;
    this.timeSeriesStorage = timeSeriesStorage;
    this.timeSeriesRecord = timeSeriesRecord;
    this.queryIncludesNamespace = queryIncludesNamespace;
    this.storageContext = timeSeriesStorage.getContext();
  }

  @Override
  public void fetchNext(Span span) {
    Span child = null;
    if (span != null) {
      child = span.newChild(AuraMetricsQueryNode.class.getName()).start();
    }
    AuraMetricsQueryResult queryResult = new AuraMetricsQueryResult(this, rollupConfig);

    String temp = config.getMetric().getMetric();
    if (queryIncludesNamespace && Strings.isNullOrEmpty(config.getNamespace())) {
      final int dotIndex = temp.indexOf(".");
      if (dotIndex >= 0) {
        // strip out the namespace
        temp = temp.substring(temp.indexOf(".") + 1);
      }
    }

    final String metric = temp;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Query metric: {}", metric);
    }
    QueryFilter queryFilter = config.getFilter();
    if (queryFilter == null && !Strings.isNullOrEmpty(config.getFilterId())) {
      queryFilter = queryResult.source().pipelineContext().query().getFilter(config.getFilterId());
    }
    Query query = QueryBuilderTSDBExt.newBuilder().fromTSDBQueryFilter(queryFilter).build();
    MetricQuery metricQuery = MetricQuery.newBuilder().withQuery(query).forMetric(metric).build();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Meta query {}", query.toString());
    }

    if (config.getFetchLast()) {
      throw new UnsupportedOperationException("TODO");
//      ResultantPointerArray result = new ResultantPointerArray(0);
//      timeSeriesStorage.readLastValue(
//          metricQuery,
//          (i, a, t) -> {
//            if (t == null) {
//              result.reuse(a);
//              int tsCount = result.getTSCount();
//              for (int x = 0; x < tsCount; x++) {
//                long tsAddress = result.getTSAddress(x);
//                long tagAddress = result.getTagAddress(x);
//                int tagLength = result.getTagLength(x);
//                byte[] tagData = new byte[tagLength];
//                Memory.read(tagAddress, tagData, tagLength);
//                timeSeriesRecord.open(tsAddress);
//                int lastTimestamp = timeSeriesRecord.getLastTimestamp();
//                double lastValue = timeSeriesRecord.getLastValue();
//                queryResult.addDP(tagData, new TimeSeriesStorage.DP(lastTimestamp, lastValue));
//                result.free();
//              }
//            } else {
//              queryResult.source().onError(t);
//            }
//          });
    } else {
      TimeSeriesQuery semanticQuery = queryResult.source().pipelineContext().query();
      int start;
      int end;
      if (config.timeShifts() != null) {
        TimeStamp ts = semanticQuery.startTime().getCopy();
        ts.subtract((TemporalAmount) config.timeShifts().getValue());
        start = (int) ts.epoch();

        ts = semanticQuery.endTime().getCopy();
        ts.subtract((TemporalAmount) config.timeShifts().getValue());
        end = (int) ts.epoch();
      } else {
        start = (int) semanticQuery.startTime().epoch();
        end = (int) semanticQuery.endTime().epoch();
      }

      long segmentTimes =
          timeSeriesStorage.read(
              metricQuery,
              start,
              end,
              (i, a, t) -> {
                if (t == null) {
                  queryResult.addAllTimeSeries(i, a);
                } else {
                  queryResult.source().onError(t);
                }
              });
      int firstSegmentTime = 0;
      int segmentCount = 0;
      if(segmentTimes > 0) {
        firstSegmentTime = storageContext.getFirstSegmentTime(segmentTimes);
        segmentCount = storageContext.getSegmentCount(segmentTimes);
      }
      queryResult.addSegmentTimes(firstSegmentTime, segmentCount);
    }

    if (child != null) {
      child.setSuccessTags().finish();
    }

    queryResult.finished();
    onNext(queryResult);
    onComplete(this, 0, 0);
  }

  @Override
  public String[] setIntervals() {
    return null;
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {}

  @Override
  public void onComplete(QueryNode downstream, long final_sequence, long total_sequences) {
    this.completeUpstream(final_sequence, total_sequences);
  }

  @Override
  public void onNext(QueryResult next) {
    sendUpstream(next);
  }

  @Override
  public void onError(Throwable t) {
    sendUpstream(t);
  }
}
