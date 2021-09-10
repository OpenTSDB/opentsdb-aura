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
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.aura.execution.AuraMetricsClusterConfig.Host;
import net.opentsdb.aura.execution.AuraMetricsClusterConfig.Hosts;
import net.opentsdb.aura.execution.AuraMetricsHealthChecker.HealthStatus;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.execution.HttpQueryV3Source;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;
import net.opentsdb.utils.SharedHttpClient;
import net.opentsdb.utils.XXHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.TemporalAmount;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * A factory for querying data from an Aura instance using HTTP(s). The factory
 * assumes each Aura instance handles a single namespace. Likewise it assumes
 * there is a data center/colo ID in the name of the Aura instance when used
 * as a multi-colo router.
 *
 */
public class AuraMetricsHttpFactory
    extends AuraHttpFactory<TimeSeriesDataSourceConfig, HttpQueryV3Source> 
    implements TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(AuraMetricsHttpFactory.class);

  public static final String TYPE = "AuraMetricsHttp";

  public static final String KEY_PREFIX = "aura.metrics.cluster.";
  public static final String ROUTER_KEY = "router";
  public static final String COLO_KEY = "colo";
  public static final String ENDPOINT_KEY = "endpoint";
  public static final String CLIENT_KEY = "client.id";
  public static final String RETENTION_KEY = "retention";
  public static final String USE_UPTIME_KEY = "use_uptime";
  public static final String HOST_MODE_KEY = "host.mode";
  public static final String FALLBACK_KEY = "fallback";
  
  public static final String BYPASS_METRIC = "aurametrics.query.bypassed";
  public static final String RETENTION_REASON = "retention";
  public static final String UPTIME_REASON = "retention";
  public static final String NS_REASON = "namespace";
  public static final String HEALTH_TYPE = "namespace";
  public static final String AURA_HOST_TAG_KEY = "aura_host";
  public static final String REASON_TAG_KEY = "reason";
  public static final String NAMESPACE_TAG_KEY = "namespace";

  public static enum HostMode {
    FAILOVER,
    ROUND_ROBIN,
  }

  private TSDB tsdb;

  // TODO - singleton config. Waste of space having dupes.
  protected volatile AuraMetricsClusterConfig config;

  protected boolean owns_healthchecker;
  protected AuraMetricsHealthChecker health_checker;
  protected RollupConfig rollup_config;
  protected HostMode host_mode;
  protected Map<Long, Pair<Integer, Host>> possible_hosts = Maps.newConcurrentMap();
  protected boolean fallback;

  @Override
  public TimeSeriesDataSourceConfig parseConfig(
      final ObjectMapper mapper, final TSDB tsdb, final JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public HttpQueryV3Source newNode(final QueryPipelineContext context, 
                                   final TimeSeriesDataSourceConfig config) {
    final long hash = hash(context, config);
    Pair<Integer, Host> pair = possible_hosts.remove(hash);
    if (pair != null) {
      return new HttpQueryV3Source(this, context, config, client, pair.getValue().uri, endpoint);
    } else {
      LOG.warn("Missing the possible host pair. Falling back.");
      final Host host = getHost(context, config);
      if (host == null) {
        context.queryContext().logError("Routed to Aura Metrics but the host was "
            + "down by the time we issued the query.");
        throw new IllegalArgumentException(
            "Failed to find an Aura host "
                + "for metric: "
                + config.getMetric().getMetric()
                + " for factory "
                + id);
      }
      return new HttpQueryV3Source(this, context, config, client, host.uri, endpoint);
    }

  }

  @Override
  public boolean supportsQuery(final QueryPipelineContext context, 
                               final TimeSeriesDataSourceConfig config) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Trying to see if an aura query is supported: " + host_mode);
    }

    if (config.getSourceId() != null && config.getSourceId().contains(":")) {
      // override.
      return true;
    }

    final Host host = getHost(context, config);
    if (host == null) {
      return false;
    }
    final long hash = hash(context, config);
    possible_hosts.put(hash, new Pair<Integer, Host>(
        (int) System.currentTimeMillis() / 1000, host));
    return true;
  }

  @Override
  public boolean supportsPushdown(final Class<? extends QueryNodeConfig> operation) {
    // TODO - Make sure Aura can handle the pushdowns somehow, e.g. via a config
    // call.
    return true;
  }

  @Override
  public RollupConfig rollupConfig() {
    return rollup_config;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    registerConfigs(tsdb);

    final String client_id = tsdb.getConfig().getString(getConfigKey(CLIENT_KEY));
    final SharedHttpClient shared_client =
        tsdb.getRegistry().getPlugin(SharedHttpClient.class, client_id);
    if (shared_client == null) {
      throw new IllegalArgumentException(
          "No shared HTTP client found "
              + "for ID: "
              + (Strings.isNullOrEmpty(client_id) ? "Default" : client_id));
    }

    // TODO - TEMP!! FIX ME!! USE CONFIG!!!
    rollup_config =
        DefaultRollupConfig.newBuilder()
            .addAggregationId("sum", 0)
            .addAggregationId("count", 1)
            .addAggregationId("min", 2)
            .addAggregationId("max", 3)
            .addAggregationId("avg", 5)
            .addAggregationId("first", 6)
            .addAggregationId("last", 7)
            .addInterval(
                DefaultRollupInterval.builder()
                    .setInterval("1h")
                    .setRowSpan("1d")
                    .setTable("ignored")
                    .setPreAggregationTable("ignored"))
            .build();
    
    client = shared_client.getClient();

    endpoint = tsdb.getConfig().getString(getConfigKey(ENDPOINT_KEY));
    host_mode = HostMode.valueOf(tsdb.getConfig().getString(getConfigKey(HOST_MODE_KEY)));
    fallback = tsdb.getConfig().getBoolean(getConfigKey(FALLBACK_KEY));
    
    // health checker singleton
    String health_checker_id = "AuraMetricsHealthCheck";
    Object shared_health_checker = tsdb.getRegistry().getSharedObject(health_checker_id);
    if (shared_health_checker != null) {
      // all set!
      health_checker = (AuraMetricsHealthChecker) shared_health_checker;
      return Deferred.fromResult(null);
    }

    health_checker = new AuraMetricsHealthChecker(tsdb, config);
    if (tsdb.getRegistry().registerSharedObject(health_checker_id, health_checker) != null) {
      // lost the race somehow. (e.g. if we load plugins in parallel)
      health_checker.shutdown();
      return Deferred.fromResult(null);
    }
    owns_healthchecker = true;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  public void run(final Timeout timeout) {
    try {
      Iterator<Entry<Long, Pair<Integer, Host>>> iterator = possible_hosts.entrySet().iterator();
      int now = (int) System.currentTimeMillis() / 1000;
      int removed = 0;
      while (iterator.hasNext()) {
        Entry<Long, Pair<Integer, Host>> entry = iterator.next();
        if (now - entry.getValue().getKey() > 300) {
          iterator.remove();
          removed++;
          LOG.warn("Removing an old possible host entry for: " + entry.getValue().getValue());
        }
      }
      LOG.warn("Removed " + removed + " entries from the posible hosts map.");
    } catch (Exception e) {
      LOG.error("Failed to run the cleanup??", e);
    }
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
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

  /** A callback for the sources that updates default_sources. */
  class SettingsCallback implements ConfigurationCallback<Object> {

    @Override
    public void update(final String key, final Object value) {
      if (key.equals(KEY_PREFIX + ROUTER_KEY)) {
        if (value == null) {
          return;
        }

        synchronized (AuraMetricsHttpFactory.this) {
          config = (AuraMetricsClusterConfig) value;
        }

        if (owns_healthchecker) {
          final boolean run_health_checks = Strings.isNullOrEmpty(
              tsdb.getConfig().getString(
                  AuraMetricsHealthChecker.KEY_PREFIX + 
                  AuraMetricsHealthChecker.READ_PATH));
          health_checker.updateConfig(config, run_health_checks);
        }
      }
    }
  }

  /**
   * Helper to register the configs.
   *
   * @param tsdb A non-null TSDB.
   */
  protected void registerConfigs(final TSDB tsdb) {
    super.registerConfigs(tsdb);
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + ROUTER_KEY)) {
      tsdb.getConfig()
          .register(
              ConfigurationEntrySchema.newBuilder()
                  .setKey(KEY_PREFIX + ROUTER_KEY)
                  .setType(AuraMetricsClusterConfig.class)
                  .setDescription("The JSON or YAML config with namespace to " + "host mappings.")
                  .isDynamic()
                  .isNullable()
                  .setSource(getClass().getName())
                  .build());
    }
    tsdb.getConfig().bind(KEY_PREFIX + ROUTER_KEY, new SettingsCallback());

    if (!tsdb.getConfig().hasProperty(getConfigKey(COLO_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(COLO_KEY), null, true, "The colo this particular instance deals with.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(ENDPOINT_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(ENDPOINT_KEY),
              "/api/query/graph",
              false,
              "Just the URI path to send queries to.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(CLIENT_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(CLIENT_KEY),
              null,
              false,
              "The ID of the SharedHttpClient plugin to use. Defaults to `null`.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(RETENTION_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(RETENTION_KEY),
              90000,
              true,
              "The amount of data, in milliseconds, retained in Aura.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(USE_UPTIME_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(USE_UPTIME_KEY),
              true,
              false,
              "Whether or not to use the uptime of the Aura host when "
                  + "determining whether or not to query it. Uses the fudge factor.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(HOST_MODE_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(HOST_MODE_KEY),
              "FAILOVER",
              true,
              "The mode to use when multiple hosts share the same namespace.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(FALLBACK_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(FALLBACK_KEY),
              true,
              true,
              "Whether or not to fallback to downstream stores if an Aura host or "
              + "two were present but out of rotation. If false, we throw an"
              + "exception.");
    }
  }
  
  Host getHost(final QueryPipelineContext context, 
               final TimeSeriesDataSourceConfig config) {
    try {
      if (config.getSourceId() != null && config.getSourceId().contains(":")) {
        // override.
        final String host = config.getSourceId().substring(
            config.getSourceId().indexOf(":") + 1);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Overriding Aura host: " + host);
        }
        if (context.query().isDebugEnabled()) {
          context.queryContext().logDebug("Aura override from query: " + host);
        }
        return new Host(host, host, host, null);
      }
      
      TimeStamp timestamp = null;
      // check for time shifts
      if (config.timeShifts() != null) {
        timestamp = config.startTimestamp().getCopy();
        timestamp.subtract((TemporalAmount) config.timeShifts().getValue());
      } else {
        timestamp = config.startTimestamp();
      }

      final String namespace =
          config
          .getMetric()
          .getMetric()
          .substring(0, config.getMetric().getMetric().indexOf("."));
      
      final Hosts hosts =
          this.config.getHosts(tsdb.getConfig().getString(getConfigKey(COLO_KEY)), namespace);

      if (hosts == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("No Aura hosts found matching namespace: " + namespace);
        }
        if (context.query().isDebugEnabled()) {
          context.queryContext().logDebug("No aura hosts found matching "
              + "namespace: " + namespace);
        }
        return null;
      }
      
      // hosts level overrides.
      final long retention = hosts.retention() != Long.MAX_VALUE ? hosts.retention :
        tsdb.getConfig().getLong(getConfigKey(RETENTION_KEY));
      
      final long now = DateTime.currentTimeMillis() / 1000;
      if (timestamp.epoch() >= (now - retention)) {
        // search
        boolean had_heartbeat = false;
        int failed = 0;
        Host possible_host = null;
        for (int i = 0; i < hosts.hosts().size(); i++) {
          final Host host = host_mode == HostMode.ROUND_ROBIN ? hosts.nextHost() : 
                hosts.hosts().get(i);
          
          // health time!
          final HealthEntry health_entry = health_checker.getEntry(host);
          if (health_entry.isOverriden()) {
            if (health_entry.status() == HealthStatus.GOOD) {
              possible_host = host;
              break;
            } else {
              if (context.query().isTraceEnabled()) {
                context.queryContext().logTrace("Host overridden and marked "
                    + "for host " + host);
              }
              continue;
            }
          }
          
          if (health_entry.status() != HealthStatus.UNKNOWN
              && health_entry.status() != HealthStatus.DOWN
              && !health_entry.isOverriden()) {
            if ((now - config.startTimestamp().epoch())
                <= (health_entry.uptime() - health_checker.uptimeFudge())) {
              // in the time so far so good. Check heartbeat
              final NamespaceHealth ns_health = health_entry.namespaceHealth(namespace);
              if (ns_health != null) {
                had_heartbeat = true;
                if (ns_health.earliest() > 0
                    && (now - config.startTimestamp().epoch())
                        <= (ns_health.earliest() - health_checker.uptimeFudge())) {
                  // WOOT! Matched!
                  if (LOG.isTraceEnabled()) {
                    LOG.trace("Matched namespace " + namespace + " to host " + host);
                  }
                  if (context.query().isDebugEnabled()) {
                    context.queryContext().logDebug("Matched namespace " 
                        + namespace + " to host " + host);
                  }
                  return host;
                }
              } else {
                possible_host = host;
              }
            } else if (context.query().isTraceEnabled()) {
              context.queryContext().logTrace("Query is beyond uptime "
                  + (health_entry.uptime() - health_checker.uptimeFudge())
                  + "s for host " + host);
            }
          } else if (health_entry.isOverriden() || 
                     health_entry.status() == HealthStatus.GOOD) {
            if (tsdb.getConfig().getBoolean(getConfigKey(USE_UPTIME_KEY))) {
              if ((now - config.startTimestamp().epoch())
                  <= (health_entry.uptime() - health_checker.uptimeFudge())) {
                possible_host = host;
                break;
              } else {
                if (LOG.isTraceEnabled()) {
                  LOG.trace(
                      "Query over uptime: "
                          + (health_entry.uptime() - health_checker.uptimeFudge())
                          + " for host "
                          + host);
                }
                if (context.query().isDebugEnabled()) {
                  context.queryContext().logDebug("Query over uptime: "
                      + (health_entry.uptime() - health_checker.uptimeFudge())
                      + " for host "
                      + host);
                }
              }
            } else {
              possible_host = host;
              break;
            }
          } else {
            if (LOG.isTraceEnabled()) {
              LOG.trace(
                  "Potential host for "
                      + namespace
                      + " was "
                      + host.host
                      + " but was marked "
                      + health_entry.status());
            }
            if (context.query().isDebugEnabled()) {
              context.queryContext().logDebug("Potential host for "
                  + namespace
                  + " was "
                  + host.host
                  + " but was marked "
                  + health_entry.status());
            }
            failed++;
          }
          
          if (possible_host != null) {
            break;
          }
        }

        if (failed < hosts.hosts.size() && !had_heartbeat && possible_host != null) {
          // we can still query a host
          if (LOG.isTraceEnabled()) {
            LOG.trace("Matched namespace " + namespace + " to host " + possible_host);
          }
          if (context.query().isDebugEnabled()) {
            context.queryContext().logDebug("Sending query for namespace " + namespace 
                + " to host " + possible_host);
          }
          return possible_host;
        } else if (LOG.isTraceEnabled()) {
          LOG.trace(
              "Nothing found for namespace "
                  + namespace
                  + "  Failed: "
                  + failed
                  + "  HB: "
                  + had_heartbeat
                  + "  PH: "
                  + possible_host);
        }
        if (context.query().isDebugEnabled()) {
          context.queryContext().logDebug("Nothing found for namespace "
              + namespace
              + "  Failed: "
              + failed
              + "  HB: "
              + had_heartbeat
              + "  PH: "
              + possible_host);
        }
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace(
              "Query over retention: " + (now - retention) + "  Now: " + now + "  Retention: " + retention);
        }
        if (context.query().isDebugEnabled()) {
          context.queryContext().logDebug("Query over retention: "
              + (now - retention) + "  Now: " + now + "  Retention: " + retention);
        }
      }
      return null;
    } catch (Throwable t) {
      LOG.error("WTF? Failed to check aura query?", t);
      return null;
    }
  }

  long hash(final QueryPipelineContext context, 
            final TimeSeriesDataSourceConfig config) {
    long hash = System.identityHashCode(context);
    hash = XXHash.updateHash(hash, config.getMetric().getMetric());
    return hash;
  }
}
