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
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.MetricQuery;
import net.opentsdb.aura.metrics.meta.Query;
import net.opentsdb.aura.metrics.QueryBuilderTSDBExt;
import net.opentsdb.data.ArrayAggregatorConfig;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.query.*;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.groupby.GroupByFactory;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.stats.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;

public class AerospikeQueryNode extends AbstractQueryNode implements TimeSeriesDataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(AerospikeQueryNode.class);

  private static ThreadLocal<double[]> SEGMENT_READ_ARRAY;
  public static final double[] IDENTITY = {0.0, 0.0, Double.MAX_VALUE, -Double.MAX_VALUE, 0.0};
  public static final ThreadLocal<double[]> DOWNSAMPLE_AGGREGATOR = ThreadLocal.withInitial(() -> Arrays.copyOf(IDENTITY, IDENTITY.length));

  private final GroupByFactory groupByFactory;
  private final LTSAerospike client;
  private TimeSeriesDataSourceConfig config;
  private boolean queryIncludesNamespace;

  private NumericArrayAggregatorFactory aggregatorFactory;
  private ArrayAggregatorConfig aggConfig;
  private GroupByConfig groupByConfig;
  private DownsampleConfig downsampleConfig;
  private RateConfig rateConfig;
  protected ArrayObjectPool doublePool;
  private int segmentsStart;
  private int segmentsEnd;
  private final int secondsInSegment;
  private final int gbQueueThreshold;
  private final int seriesPerJob;
  private final int gbThreads;
  private String[] gbKeys;

  public AerospikeQueryNode(
          final AerospikeSourceFactory factory,
          final QueryPipelineContext context,
          final TimeSeriesDataSourceConfig config,
          final boolean queryIncludesNamespace) {
    super(factory, context);
    this.config = config;
    this.queryIncludesNamespace = queryIncludesNamespace;

    AerospikeClientPlugin clientPlugin = context.tsdb().getRegistry().getDefaultPlugin(AerospikeClientPlugin.class);
    if (clientPlugin == null) {
      throw new IllegalStateException("No plugin for the AS Client?");
    }
    client = clientPlugin.asClient();

    secondsInSegment = pipelineContext().tsdb().getConfig().getInt(AerospikeSourceFactory.SECONDS_IN_SEGMENT_KEY);
    groupByFactory = (GroupByFactory) pipelineContext().tsdb().getRegistry().getQueryNodeFactory(GroupByFactory.TYPE);
    gbQueueThreshold = pipelineContext().tsdb().getConfig().getInt(GroupByFactory.GROUPBY_QUEUE_THRESHOLD_KEY);
    seriesPerJob = pipelineContext().tsdb().getConfig().getInt(GroupByFactory.GROUPBY_TIMESERIES_PER_JOB_KEY);
    gbThreads = pipelineContext().tsdb().getConfig().getInt(GroupByFactory.GROUPBY_THREAD_COUNT_KEY);

    doublePool = (ArrayObjectPool) pipelineContext().tsdb().getRegistry().getObjectPool(
            DoubleArrayPool.TYPE);

    // IMPORTANT to initialize the array.
    if (SEGMENT_READ_ARRAY == null) {
      synchronized(AerospikeQueryNode.class) {
        if (SEGMENT_READ_ARRAY == null) {
          SEGMENT_READ_ARRAY = ThreadLocal.withInitial(() -> new double[secondsInSegment]);
        }
      }
    }
  }

  @Override
  public Deferred<Void> initialize(Span span) {
    return super.initialize(span)
            .addCallback(
                    arg -> {
                      if (config.getPushDownNodes() != null) {
                        for (QueryNodeConfig queryNodeConfig :
                                (List<QueryNodeConfig>) config.getPushDownNodes()) {
                          if (queryNodeConfig instanceof DownsampleConfig) {
                            downsampleConfig = (DownsampleConfig) queryNodeConfig;
                          } else if (queryNodeConfig instanceof GroupByConfig) {
                            groupByConfig = (GroupByConfig) queryNodeConfig;
                          } else if (queryNodeConfig instanceof RateConfig) {
                            rateConfig = (RateConfig) queryNodeConfig;
                          }
                        }
                      }

                      if (groupByConfig == null) {
                        LOGGER.info("No group by config");
                        // return Deferred.fromError(new IllegalStateException("No group by config
                        // found."));
                      } else {
                        aggregatorFactory =
                                pipelineContext()
                                        .tsdb()
                                        .getRegistry()
                                        .getPlugin(
                                                NumericArrayAggregatorFactory.class, groupByConfig.getAggregator());
                        if (groupByConfig.getTagKeys() != null) {
                          gbKeys = new String[groupByConfig.getTagKeys().size()];
                          int idx = 0;
                          for (String key : groupByConfig.getTagKeys()) {
                            gbKeys[idx++] = key;
                          }
                        }
                      }

                      if (downsampleConfig != null) {
                        aggConfig =
                                DefaultArrayAggregatorConfig.newBuilder()
                                        .setArraySize(downsampleConfig.intervals())
                                        //.setInfectiousNaN(groupByConfig.getInfectiousNan()) // TODO - fix GB nan
                                        .build();
                      }

                      // compute query timestamps.
                      segmentsStart = (int) pipelineContext().query().startTime().epoch();
                      segmentsStart = segmentsStart - (segmentsStart % secondsInSegment);

                      segmentsEnd = (int) pipelineContext().query().endTime().epoch();
                      segmentsEnd = segmentsEnd - (segmentsEnd % secondsInSegment);
                      if (segmentsEnd < (int) pipelineContext().query().endTime().epoch()) {
                        segmentsEnd += secondsInSegment; // always exclusive and may be in the future.
                      }

                      LOGGER.info("Instantiated AS query node from {} to {}", segmentsStart, segmentsEnd);
                      LOGGER.info(
                              "Has GB {} Has DS {} Has Rate {}", (groupByConfig != null), (downsampleConfig != null), (rateConfig != null));
                      return null;
                    });
  }

  @Override
  public void fetchNext(Span span) {
    Span child = null;
    if (span != null) {
      child = span.newChild(AerospikeQueryNode.class.getName()).start();
    }

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
      queryFilter = pipelineContext().query().getFilter(config.getFilterId());
    }
    Query query = QueryBuilderTSDBExt.newBuilder().fromTSDBQueryFilter(queryFilter).build();
    MetricQuery metricQuery = MetricQuery.newBuilder().withQuery(query).forMetric(metric).build();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Meta query {}", query.toString());
    }

    final MetaTimeSeriesQueryResult mqr;
    if (config.getFetchLast()) {
      throw new UnsupportedOperationException();
    } else {
      TimeSeriesQuery semanticQuery = pipelineContext().query();
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

      // TODO - this is where we run the meta query.
      //mqr = ((AerospikeSourceFactory) factory()).timeSeriesStorage().search(metricQuery, start, end, gbKeys);
      mqr = null;
    }

    LOGGER.info("FINISHED with meta query: " + mqr.numGroups());
    if (child != null) {
      child.setSuccessTags().finish();
    }
    final AerospikeQueryResult result;
    if (groupByConfig != null) {
      result = new AerospikeGBQueryResult(this, mqr);
    } else {
      result = new AerospikeQueryResult(this, mqr);
    }
    onNext(result);

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

  public int queueThreshold() {
    return gbQueueThreshold;
  }

  public int seriesPerJob() {
    return seriesPerJob;
  }

  public int getGbThreads() {
    return gbThreads;
  }

  public String[] gbKeys() {
    return gbKeys;
  }

  public GroupByFactory groupByFactory() {
    return groupByFactory;
  }

  public ArrayAggregatorConfig aggConfig() {
    return aggConfig;
  }

  public GroupByConfig groupByConfig() {
    return groupByConfig;
  }

  public DownsampleConfig downsampleConfig() {
    return downsampleConfig;
  }

  public RateConfig rateConfig() {
    return rateConfig;
  }

  public NumericArrayAggregator newAggregator() {
    return (NumericArrayAggregator) aggregatorFactory.newAggregator(aggConfig);
  }

  public ArrayObjectPool doublePool() {
    return doublePool;
  }

  public LTSAerospike asClient() {
    return client;
  }

  public int getSegmentsStart() {
    return segmentsStart;
  }

  public int getSegmentsEnd() {
    return segmentsEnd;
  }

  public int getSecondsInSegment() {
    return secondsInSegment;
  }

  public double[] getSegmentReadArray() {
    return SEGMENT_READ_ARRAY.get();
  }
}