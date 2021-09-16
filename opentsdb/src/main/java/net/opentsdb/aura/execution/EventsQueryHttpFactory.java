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

package net.opentsdb.aura.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.execution.BaseHttpExecutorFactory;
import net.opentsdb.query.execution.HttpQueryV3Source;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.SharedHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.util.List;

public class EventsQueryHttpFactory<C extends TimeSeriesDataSourceConfig, N extends TimeSeriesDataSource>
    extends BaseHttpExecutorFactory<C, HttpQueryV3Source> {

  protected String id;
  protected String host;
  protected String endpoint;
  protected volatile CloseableHttpAsyncClient client;
  public static final String TYPE = "EventsQueryV3";
  public static final String KEY_PREFIX = "events.";
  public static final String HOST = "host";


  @Override
  public void setupGraph(final QueryPipelineContext query, final C config,
      final QueryPlanner planner) {}

  @Override
  public boolean supportsQuery(final QueryPipelineContext context, C config) {
    return true;
  }

  @Override
  public boolean supportsPushdown(Class<? extends QueryNodeConfig> operation) {
    return true;
  }

  @VisibleForTesting
  String getConfigKey(final String suffix) {
    if (id == null || id.equals(TYPE)) { // yes, same addy here.
      return KEY_PREFIX + suffix;
    } else {
      return KEY_PREFIX + id + "." + suffix;
    }
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.id = id;

    registerConfigs(tsdb);

    final String client_id = null;
    final SharedHttpClient shared_client =
        tsdb.getRegistry().getPlugin(SharedHttpClient.class, client_id);
    if (shared_client == null) {
      throw new IllegalArgumentException("No shared HTTP client found " + "for ID: "
          + (Strings.isNullOrEmpty(client_id) ? "Default" : client_id));
    }
    client = shared_client.getClient();
    host = tsdb.getConfig().getString(getConfigKey(HOST));
    endpoint = tsdb.getConfig().getString(getConfigKey(ENDPOINT_KEY));

    return Deferred.fromResult(null);
  }

  protected void registerConfigs(final TSDB tsdb) {
    super.registerConfigs(tsdb);
    if (!tsdb.getConfig().hasProperty(getConfigKey(HOST))) {
      tsdb.getConfig().register(getConfigKey(HOST), "http://stg-eventdb-1.yms.gq1.yahoo.com:4080",
          true, "Events rotation node");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(ENDPOINT_KEY))) {
      tsdb.getConfig().register(getConfigKey(ENDPOINT_KEY), "/api/query/graph", true, "Events endpoint");
    }
  }

  @Override
  public RollupConfig rollupConfig() {
    return null;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public C parseConfig(ObjectMapper mapper, TSDB tsdb, JsonNode node) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HttpQueryV3Source newNode(QueryPipelineContext context, C config) {
    return new HttpQueryV3Source(this, context, config, client, host, endpoint);
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public HttpQueryV3Source newNode(QueryPipelineContext queryPipelineContext) {
    throw new UnsupportedOperationException("Node config required.");
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(final TimeSeriesByteId id, final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(final List<String> join_keys, final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(final List<String> join_metrics,
      final Span span) {
    return Deferred.fromError(new UnsupportedOperationException());
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

}
