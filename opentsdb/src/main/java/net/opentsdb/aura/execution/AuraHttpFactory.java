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

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.execution.BaseHttpExecutorFactory;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.stats.Span;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.util.List;

/**
 * A base factory for querying Aura metrics hosts.
 *
 * @since 3.0
 */
public abstract class AuraHttpFactory<
        C extends TimeSeriesDataSourceConfig, N extends TimeSeriesDataSource>
    extends BaseHttpExecutorFactory<C, N> {

  protected String id;
  protected String host;
  protected String endpoint;
  protected volatile CloseableHttpAsyncClient client;

  @Override
  public void setupGraph(
      final QueryPipelineContext query, final C config, final QueryPlanner planner) {
    // TODO
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public N newNode(QueryPipelineContext queryPipelineContext) {
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
  public Deferred<List<byte[]>> encodeJoinMetrics(
      final List<String> join_metrics, final Span span) {
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
