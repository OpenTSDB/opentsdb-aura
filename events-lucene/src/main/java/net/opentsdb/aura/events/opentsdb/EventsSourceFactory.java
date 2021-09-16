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

package net.opentsdb.aura.events.opentsdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.events.ConfigUtils;
import net.opentsdb.aura.events.lucene.LuceneWriter;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;

import java.util.List;

public class EventsSourceFactory extends BaseTSDBPlugin
    implements TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, EventsQueryNode> {

  public static final String TYPE = "Events";
  public static final String SHARED_ID = "events.lucene.shared.writer";

  protected LuceneWriter writer;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;

    String configId = ConfigUtils.configId(id, SHARED_ID);
    if (!tsdb.getConfig().hasProperty(configId)) {
      tsdb.getConfig().register(configId, LuceneWriter.TYPE, false, "TODO");
    }

    Object obj = tsdb.getRegistry().getSharedObject(tsdb.getConfig().getString(configId));
    if (obj == null) {
      return Deferred.fromError(new IllegalArgumentException(
              "No shared writer object for config " + configId));
    }
    if (!(obj instanceof LuceneWriter)) {
      return Deferred.fromError(new IllegalArgumentException(
              "Shared object for config " + configId + " was not a LuceneWriter: " + obj.getClass()));
    }
    writer = (LuceneWriter) obj;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return net.opentsdb.common.Const.TS_STRING_ID;
  }

  @Override
  public boolean supportsQuery(QueryPipelineContext context,
                               TimeSeriesDataSourceConfig timeSeriesDataSourceConfig) {
    return true;
  }

  @Override
  public boolean supportsPushdown(Class<? extends QueryNodeConfig> aClass) {
    if (aClass.equals(GroupByConfig.class)) {
      return true;
    }
    return false;
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId timeSeriesByteId, Span span) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(List<String> list, Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(List<String> list, Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  @Override
  public RollupConfig rollupConfig() {
    return null;
  }

  @Override
  public TimeSeriesDataSourceConfig parseConfig(ObjectMapper mapper, TSDB tsdb, JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(final QueryPipelineContext context,
      final TimeSeriesDataSourceConfig config,
      final QueryPlanner planner) {
    for (QueryNodeConfig conf : planner.configGraph().predecessors(config)) {
      if (conf instanceof DownsampleConfig) {
        TimeSeriesDataSourceConfig.Builder builder = (TimeSeriesDataSourceConfig.Builder) config
            .toBuilder();
        builder.addPushDownNode(conf);
        planner.replace(config, builder.build());
        break;
      }
    }
  }

  @Override
  public EventsQueryNode newNode(QueryPipelineContext queryPipelineContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EventsQueryNode newNode(QueryPipelineContext context,
                                 TimeSeriesDataSourceConfig config) {
    return new EventsQueryNode(this, context, config);
  }

  LuceneWriter writer() {
    return writer;
  }
}
