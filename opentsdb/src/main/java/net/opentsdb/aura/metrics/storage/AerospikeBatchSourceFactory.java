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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.meta.grpc.MetaGrpcClient;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.BaseTimeSeriesDataSourceConfig;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.plan.DefaultQueryPlanner;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AerospikeBatchSourceFactory extends BaseTSDBPlugin
        implements TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, AerospikeBatchQueryNode> {

  private static final Logger LOGGER  = LoggerFactory.getLogger(AerospikeBatchSourceFactory.class);
  public static final String TYPE = "AerospikeBatchSourceFactory";

  public static final String SECONDS_IN_SEGMENT_KEY = "aura.secondsInSegment";

  private static String DEFAULT_ROLLUP_KEY = "opentsdb.http.executor.default.rollups.config";
  private static String META_HOST_KEY = "meta.grpc.host";
  private static String META_HOST_PORT_KEY = "meta.grpc.port";
  public static String AS_BATCH_LIMIT_KEY = "aerospike.batch.limit";
  public static String AS_JOBS_PER_QUERY = "aerospike.jobs.per.query";

  private RollupConfig rollupConfig;

  private TimeSeriesStorageIf timeSeriesStorage;
  private TimeSeriesRecordFactory timeSeriesRecordFactory;
  private boolean queryIncludesNamespace;
  private MetaGrpcClient metaGrpcClient;

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    registerLocalConfigs(tsdb);

    rollupConfig =
            tsdb.getConfig()
                    .getTyped(DEFAULT_ROLLUP_KEY, DefaultRollupConfig.class);

    metaGrpcClient = new MetaGrpcClient(tsdb.getConfig().getString(META_HOST_KEY),
            tsdb.getConfig().getInt(META_HOST_PORT_KEY));

    return new AerospikeBatchJobAllocator().initialize(tsdb, null);
  }

  private void registerLocalConfigs(TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(DEFAULT_ROLLUP_KEY)) {
      tsdb.getConfig()
              .register(
                      ConfigurationEntrySchema.newBuilder()
                              .setKey(DEFAULT_ROLLUP_KEY)
                              .setType(DefaultRollupConfig.class)
                              .setDescription("The default roll up config.")
                              .isNullable()
                              .setSource(getClass().getName())
                              .build());
    }

    if (!tsdb.getConfig().hasProperty(META_HOST_KEY)) {
      tsdb.getConfig().register(META_HOST_KEY, "10.214.168.140", false, "TODO");
    }
    if (!tsdb.getConfig().hasProperty(META_HOST_PORT_KEY)) {
      tsdb.getConfig().register(META_HOST_PORT_KEY, 9999, false, "TODO");
    }

    if (!tsdb.getConfig().hasProperty(AS_BATCH_LIMIT_KEY)) {
      tsdb.getConfig().register(AS_BATCH_LIMIT_KEY, 1024, true,
              "The number of keys to send in one batch.");
    }
    if (!tsdb.getConfig().hasProperty(AS_JOBS_PER_QUERY)) {
      tsdb.getConfig().register(AS_JOBS_PER_QUERY, 16, true,
              "How many jobs to submit per query to the query thread pool.");
    }
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0";
  }

  @Override
  public boolean supportsPushdown(
          final Class<? extends QueryNodeConfig> operation) {
    if (operation == DownsampleConfig.class ||
            operation == RateConfig.class ||
            operation == GroupByConfig.class) {
      return true;
    }
    return false;
  }

  @Override
  public TimeSeriesDataSourceConfig parseConfig(final ObjectMapper mapper,
                                                final TSDB tsdb,
                                                final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(final QueryPipelineContext context,
                         final TimeSeriesDataSourceConfig config,
                         final QueryPlanner planner) {
    if (config.hasBeenSetup()) {
      // all done.
      return;
    }

    planner.baseSetupGraph(context, config);
  }

  @Override
  public AerospikeBatchQueryNode newNode(QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AerospikeBatchQueryNode newNode(final QueryPipelineContext context,
                                         final TimeSeriesDataSourceConfig config) {
    return new AerospikeBatchQueryNode(this, context, config, queryIncludesNamespace);
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId id, Span span) {
    try {
      final BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId.newBuilder()
              .setMetric(new String(id.metric(), Const.UTF8_CHARSET));

      if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
        for (int i = 0; i < id.aggregatedTags().size(); i++) {
          builder.addAggregatedTag(new String(id.aggregatedTags().get(i),
                  Const.UTF8_CHARSET));
        }
      }

      if (id.tags() != null) {
        for (final Map.Entry<byte[], byte[]> entry : id.tags().entrySet()) {
          builder.addTags(new String(entry.getKey(), Const.UTF8_CHARSET),
                  new String(entry.getValue(), Const.UTF8_CHARSET));
        }
      }

      if (id.disjointTags() != null) {
        for (int i = 0; i < id.disjointTags().size(); i++) {
          builder.addAggregatedTag(new String(id.disjointTags().get(i),
                  Const.UTF8_CHARSET));
        }
      }

      return Deferred.fromResult(builder.build());
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(List<String> join_keys, Span span) {
    final List<byte[]> keys = Lists.newArrayListWithCapacity(join_keys.size());
    for (int i = 0; i < join_keys.size(); i++) {
      keys.add(join_keys.get(i).getBytes(Const.UTF8_CHARSET));
    }
    return Deferred.fromResult(keys);
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(List<String> join_metrics,
                                                  Span span) {
    final List<byte[]> as_bytes = Lists.newArrayListWithCapacity(join_metrics.size());
    for (int i = 0; i < join_metrics.size(); i++) {
      as_bytes.add(join_metrics.get(i).getBytes(Const.UTF8_CHARSET));
    }
    return Deferred.fromResult(as_bytes);
  }

  @Override
  public boolean supportsQuery(QueryPipelineContext context,
                               TimeSeriesDataSourceConfig config) {
    return true;
  }

  @Override
  public RollupConfig rollupConfig() {
    return rollupConfig;
  }

  boolean recursiveAddTimeShift(final QueryPlanner planner,
                                final TimeSeriesDataSourceConfig config,
                                final QueryNodeConfig current) {
    if (planner.configGraph().predecessors(current).isEmpty()) {
      // either everything is pushed down or we only have a shift query.
      final BaseQueryNodeConfig shift_config =
              TimeShiftConfig.newBuilder()
                      .setTimeshiftInterval(config.getTimeShiftInterval())
                      .setId(config.getId() + "-timeShift")
                      .build();

      final Set<QueryNodeConfig> predecessors = Sets.newHashSet(
              planner.configGraph().predecessors(current));
      for (final QueryNodeConfig predecessor : predecessors) {
        planner.addEdge(predecessor, shift_config);
        planner.removeEdge(predecessor, current);
      }
      planner.addEdge(shift_config, current);
      if (((DefaultQueryPlanner) planner).sinkFilters().containsKey(current.getId())) {
        ((DefaultQueryPlanner) planner).sinkFilters().remove(current.getId());
        ((DefaultQueryPlanner) planner).sinkFilters().put(shift_config.getId(), null);
      }
      return true;
    }

    for (final QueryNodeConfig upstream : planner.configGraph().predecessors(current)) {
      if (!supportsPushdown(upstream.getClass())) {
        final BaseQueryNodeConfig shift_config =
                TimeShiftConfig.newBuilder()
                        .setTimeshiftInterval(config.getTimeShiftInterval())
                        .setId(config.getId() + "-timeShift")
                        .build();

        final Set<QueryNodeConfig> predecessors = Sets.newHashSet(
                planner.configGraph().predecessors(current));
        for (final QueryNodeConfig predecessor : predecessors) {
          planner.addEdge(predecessor, shift_config);
          planner.removeEdge(predecessor, current);
        }
        planner.addEdge(shift_config, current);
        if (((DefaultQueryPlanner) planner).sinkFilters().containsKey(current.getId())) {
          ((DefaultQueryPlanner) planner).sinkFilters().remove(current.getId());
          ((DefaultQueryPlanner) planner).sinkFilters().put(shift_config.getId(), null);
        }
        return true;
      }

      if (recursiveAddTimeShift(planner, config, upstream)) {
        return true;
      }
    }
    return false;
  }

  public void setTimeSeriesStorage(TimeSeriesStorageIf timeSeriesStorage) {
    this.timeSeriesStorage = timeSeriesStorage;
  }

  public void setTimeSeriesRecordFactory(TimeSeriesRecordFactory timeSeriesRecordFactory) {
    this.timeSeriesRecordFactory = timeSeriesRecordFactory;
  }

  public void setQueryIncludesNamespace(boolean queryIncludesNamespace) {
    this.queryIncludesNamespace = queryIncludesNamespace;
  }

  public MetaGrpcClient metaGrpcClient() {
    return metaGrpcClient;
  }

  TimeSeriesStorageIf timeSeriesStorage() {
    return timeSeriesStorage;
  }

}