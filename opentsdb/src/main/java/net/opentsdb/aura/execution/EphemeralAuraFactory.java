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
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.metrics.meta.endpoints.AuraMetricsStatefulSetRegistry;
import net.opentsdb.aura.metrics.meta.endpoints.ShardEndPoint;
import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesDataSourceConfig.Builder;
import net.opentsdb.query.execution.HttpQueryV3Source;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *     Use the TimeRouter like this
 *     now - 2h = shard set 1  points to THIS
 *     2h - 4h = shard set 2  points to THIS
 *     4h - 2w = AS
 *     2w - 1y = HBase
 *
 *     1) Find the namespace for this query
 *     2) Find the shard endpoints
 *     3) Instantiate a merger
 *     4) Link to merger
 *
 *     Pushdowns should be present.
 * TODO - some more work on this and tie in with the real discovery service.
 */
public class EphemeralAuraFactory
        extends BaseTSDBPlugin
        implements TimeSeriesDataSourceFactory<TimeSeriesDataSourceConfig, TimeSeriesDataSource> {
  private static final Logger LOG = LoggerFactory.getLogger(EphemeralAuraFactory.class);

  public static final String KEY_PREFIX = "aura.metrics.ephemeral.";
  public static final String SOURCE_ID = "source.id";
  public static final String START = "time.end";
  public static final String END = "time.start";
  public static final String DISCOVERY_ID = "discovery.service.id";
  public static final String SERVICE_KEY = "discovery.service.key";

  public static final String TYPE = "EphemeralAuraMetricsHttp";

  protected AuraMetricsHttpFactory factory;
  protected AuraMetricsStatefulSetRegistry discoveryService;
  protected String serviceKey;
  protected long relativeStart;
  protected long relativeEnd;
  private final Random random = new Random();
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;

    if (!tsdb.getConfig().hasProperty(getConfigKey(SOURCE_ID))) {
      tsdb.getConfig().register(getConfigKey(SOURCE_ID), null, false,
              "A non-null Aura source ID to use for calling out to " +
                      "the individual Aura pods once the endpoints are discovered.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(DISCOVERY_ID))) {
      tsdb.getConfig().register(getConfigKey(DISCOVERY_ID), null, false,
              "A non-null K8s discovery service plugin ID.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(SERVICE_KEY))) {
      tsdb.getConfig().register(getConfigKey(SERVICE_KEY), "AuraMetrics", false,
              "The service key in the discoverability config for metrics.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(START))) {
      tsdb.getConfig().register(getConfigKey(START), null, false,
              "The relative start time for data to be present in these " +
                      "shards. Must be a TSDB duration, e.g. '1h'");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(END))) {
      tsdb.getConfig().register(getConfigKey(END), null, false,
              "The relative end time for data to be present in these " +
                      "shards. Must be a TSDB duration, e.g. '1h'. May be null for tip data.");
    }

    String temp = tsdb.getConfig().getString(getConfigKey(START));
    if (Strings.isNullOrEmpty(temp)) {
      LOG.error("No {} configured.", getConfigKey(START));
      return Deferred.fromError(new IllegalArgumentException(
              "The " + getConfigKey(START) + " must be set and valid."));
    }
    relativeStart = DateTime.parseDuration(temp) / 1000;

    temp = tsdb.getConfig().getString(getConfigKey(END));
    if (!Strings.isNullOrEmpty(temp)) {
      relativeEnd = DateTime.parseDuration(temp) / 1000;
    }

    String sourceId = tsdb.getConfig().getString(getConfigKey(SOURCE_ID));
    if (Strings.isNullOrEmpty(sourceId)) {
      LOG.error("No {} configured.", getConfigKey(SOURCE_ID));
      return Deferred.fromError(new IllegalArgumentException(
              "The " + getConfigKey(SOURCE_ID) + " must be set and valid."));
    }

    factory = tsdb.getRegistry().getPlugin(AuraMetricsHttpFactory.class, sourceId);
    if (factory == null) {
      LOG.error("No AuraMetricsHttpFactory found for source ID {}", sourceId);
      return Deferred.fromError(new IllegalArgumentException(
              "No AuraMetricsHttpFactory found for source ID " + sourceId));
    }

    // could be null
    String discoveryId = tsdb.getConfig().getString(getConfigKey(DISCOVERY_ID));
    discoveryService = tsdb.getRegistry().getPlugin(AuraMetricsStatefulSetRegistry.class, discoveryId);
    if (discoveryService == null) {
      LOG.error("No AuraMetricsDiscoveryService found for source ID {}",
              discoveryId == null ? "default" : discoveryId);
      return Deferred.fromError(new IllegalArgumentException(
              "No AuraMetricsDiscoveryService found for source ID " +
                      (discoveryId == null ? "default" : discoveryId)));
    }
    serviceKey = tsdb.getConfig().getString(getConfigKey(SERVICE_KEY));

    LOG.info("Successfully initialized Ephemeral Aura Source Factory with ID {} and service key {}",
            (id == null ? "default" : id),
            serviceKey);
    return Deferred.fromResult(null);
  }

  @Override
  public void setupGraph(final QueryPipelineContext context,
                         final TimeSeriesDataSourceConfig config,
                         final QueryPlanner planner) {
    final String namespace = config.getMetric().getMetric()
                    .substring(0, config.getMetric().getMetric().indexOf("."));

    final long now = DateTime.currentTimeMillis() / 1000;
    final Map<String, List<ShardEndPoint>> services = discoveryService.getEndpoints(
            namespace, now - relativeStart);
    final List<ShardEndPoint> shards = services.get(serviceKey);

    LOG.info("Received end points: {} {}", services, shards);

    if (shards == null || shards.isEmpty()) {
      throw new IllegalStateException("Unable to find shards for namespace "
              + namespace + " and service " + serviceKey);
    }
    List<TimeSeriesDataSourceConfig.Builder> sources = Lists.newArrayListWithExpectedSize(shards.size());
    List<String> ids = Lists.newArrayListWithExpectedSize(shards.size());
    List<String> timeouts = Lists.newArrayListWithExpectedSize(shards.size());
    for (int i = 0; i < shards.size(); i++) {
      final ShardEndPoint shard = shards.get(i);
      final TimeSeriesDataSourceConfig.Builder builder = (Builder) config.toBuilder();
      sources.add(builder);
      final String newId = config.getId() + "_shard_" + i;
      ids.add(newId);
      builder.setSources(Lists.newArrayList(factory.id() +
              ":https://" + shard.getHost() + ":" + shard.getPort()))
              .setId(newId)
              .setResultIds(Lists.newArrayList(new DefaultQueryResultId(
                      newId, newId)));
      timeouts.add("30s"); // TODO - config
    }

    MergerConfig merger = (MergerConfig) MergerConfig.newBuilder()
            .setAggregator(groupBy(config))
            .setMode(MergerConfig.MergeMode.SHARD)
            .setTimeouts(timeouts)
            .setSortedDataSources(ids)
            .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                    .setFillPolicy(FillPolicy.NONE)
                    .setRealFillPolicy(FillWithRealPolicy.NONE)
                    .setDataType(NumericType.TYPE.toString())
                    .build())
            .setDataSource(config.getId())
            .setId(config.getId())
            .build();
    planner.replace(config, merger);
    for (final TimeSeriesDataSourceConfig.Builder builder : sources) {
      planner.addEdge(merger, builder.build());
    }
  }

  @Override
  public TimeSeriesDataSource newNode(QueryPipelineContext queryPipelineContext) {
    return null;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public boolean supportsQuery(final QueryPipelineContext context,
                               final TimeSeriesDataSourceConfig config) {
    final String namespace = config.getMetric().getMetric()
            .substring(0, config.getMetric().getMetric().indexOf("."));
    final List<ShardEndPoint> shards = pickEndpoints(discoveryService.getEndpoints(namespace));
    if (shards == null || shards.isEmpty()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean supportsPushdown(Class<? extends QueryNodeConfig> aClass) {
    // TODO - Make sure Aura can handle the pushdowns somehow, e.g. via a config
    // call.
    return true;
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId timeSeriesByteId, Span span) {
    return null;
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(List<String> list, Span span) {
    return Deferred.fromError(new UnsupportedOperationException("Shouldn't be here"));
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(List<String> list, Span span) {
    return Deferred.fromError(new UnsupportedOperationException("Shouldn't be here"));
  }

  @Override
  public RollupConfig rollupConfig() {
    return factory.rollupConfig();
  }

  @Override
  public TimeSeriesDataSourceConfig parseConfig(final ObjectMapper mapper,
                                                final TSDB tsdb,
                                                final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public HttpQueryV3Source newNode(final QueryPipelineContext queryPipelineContext,
                                   final TimeSeriesDataSourceConfig timeSeriesDataSourceConfig) {
    throw new UnsupportedOperationException("Should not be here.");
  }

  @Override
  public String type() {
    return TYPE;
  }

  String groupBy(TimeSeriesDataSourceConfig config) {
    List<QueryNodeConfig> pushdowns = config.getPushDownNodes();
    if (pushdowns == null) {
      return null;
    }

    for (int i = 0; i < pushdowns.size(); i++) {
      QueryNodeConfig nodeConfig = pushdowns.get(i);
      if (nodeConfig instanceof GroupByConfig) {
        return ((GroupByConfig) nodeConfig).getAggregator();
      }
    }
    return null;
  }

  /**
   * Helper to build the config key with a factory id.
   *
   * @param suffix The non-null and non-empty config suffix.
   * @return The key containing the id.
   */
  @VisibleForTesting
  String getConfigKey(final String suffix) {
    if (id == null || id == TYPE) { // yes, same addy here.
      return KEY_PREFIX + suffix;
    } else {
      return KEY_PREFIX + id + "." + suffix;
    }
  }

  private List<ShardEndPoint> pickEndpoints(Map<String, List<ShardEndPoint>> endpointsMap) {
    final int i = random.nextInt(endpointsMap.size());
    int j = 0;
    for(String key: endpointsMap.keySet()) {
      if (j == i) {
        return endpointsMap.get(key);
      }
      j++;
    }

    return null;
  }

}
