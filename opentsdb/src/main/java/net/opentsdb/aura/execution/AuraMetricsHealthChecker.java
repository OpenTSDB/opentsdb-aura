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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.CharSource;
import com.google.common.io.Files;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.aura.execution.AuraMetricsClusterConfig.Host;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.DefaultSharedHttpClient;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Threads;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * TODO - need to implement the heartbeats better.
 */
public class AuraMetricsHealthChecker {
  private static final Logger LOG = LoggerFactory.getLogger(AuraMetricsHealthChecker.class);
  
  public static final String KEY_PREFIX = "aura.cluster.healthcheck.";
  public static final String WRITE_PATH = "health.file.write_path";
  public static final String FILE_LOAD_INTERVAL = "health.file.interval";
  public static final String READ_PATH = "health.file.read_path";
  public static final String IO_THREADS = "health.io.threads";
  public static final String HEALTH_ENDPOINT_KEY = "endpoint";
  public static final String HEALTH_INTERVAL_KEY = "health.interval";
  public static final String HEARTBEATS_ENDPOINT_KEY = "heartbeats.endpoint";
  public static final String HEARTBEATS_KEY = "heartbeats.enabled";
  public static final String HEARTBEATS_INTERVAL_KEY = "heartbeats.interval";
  public static final String TIME_TAKEN_KEY = "threshold.time_taken";
  public static final String SLOW_KEY = "threshold.slow";
  public static final String DROP_KEY = "threshold.drop_rate";
  public static final String LAG_KEY = "threshold.lag";
  public static final String WRITE_KEY = "threshold.write_rate";
  public static final String UPTIME_KEY = "uptime.fudge";
  public static final String OVERRIDE_ENABLE_KEY = "override.enable";
  
  public static final String STATUS_METRIC = "aura.health.status";
  public static final String OK_CAUSE = "OK";
  
  static enum HealthStatus {
    UNKNOWN,
    GOOD,
    DOWN,
    SLOW
  }

  public static enum HealthCheckOverride {
    ENABLED,
    DISABLED,
    AUTO
  }
  
  private final TSDB tsdb;
  private final CloseableHttpAsyncClient client;
  private final int interval;
  private volatile boolean heartbeats_enabled;
  private final int heartbeats_interval;
  private final HashedWheelTimer timer;
  protected Map<Host, HealthEntry> health;
  protected Map<Host, Timeout> health_timeouts;
  protected Map<Host, Timeout> heartbeats_timeouts;
  
  public AuraMetricsHealthChecker(final TSDB tsdb, 
                                  final AuraMetricsClusterConfig config) {
    this.tsdb = tsdb;
    registerConfigs(tsdb);
    
    // NOTE that we have our own client and timer here as we may be polling
    // pretty often and don't want to affect the query pools and timer.
    final int io_threads = tsdb.getConfig().getInt(KEY_PREFIX + IO_THREADS);
    client = HttpAsyncClients.custom()
        .setDefaultIOReactorConfig(IOReactorConfig.custom()
            .setIoThreadCount(io_threads)
            .setConnectTimeout(10000)
            .setSoKeepAlive(true)
            .setTcpNoDelay(true)
            .setSoTimeout(10000)
            .build())
        .setMaxConnTotal(16)
        .setMaxConnPerRoute(1)
        .build();
    client.start();
    
    interval = tsdb.getConfig().getInt(KEY_PREFIX + HEALTH_INTERVAL_KEY);
    heartbeats_interval = tsdb.getConfig().getInt(KEY_PREFIX + HEARTBEATS_INTERVAL_KEY);
    timer = Threads.newTimer("AuraMetricsHealthChecker");
    timer.start();
    health = Maps.newConcurrentMap();
    health_timeouts = Maps.newConcurrentMap();
    heartbeats_timeouts = Maps.newConcurrentMap();
    
    final String write_path = tsdb.getConfig().getString(KEY_PREFIX + WRITE_PATH);
    final boolean run_health_checks = Strings.isNullOrEmpty(
        tsdb.getConfig().getString(KEY_PREFIX + READ_PATH)) || 
        !Strings.isNullOrEmpty(write_path);
    updateConfig(config, run_health_checks);
    if (!run_health_checks) {
      LOG.info("Not running health checks from this host. Scheduling load of "
          + "status from: " + tsdb.getConfig().getString(KEY_PREFIX + READ_PATH));
      tsdb.getMaintenanceTimer().newTimeout(new ReadStateTask(), 
          tsdb.getConfig().getLong(KEY_PREFIX + FILE_LOAD_INTERVAL), 
          TimeUnit.MILLISECONDS);
    }
    
    if (!Strings.isNullOrEmpty(write_path)) {
      LOG.info("Configured to run health checks and write state to: " + write_path);
      tsdb.getMaintenanceTimer().newTimeout(new WriteFileTask(), 
          tsdb.getConfig().getLong(KEY_PREFIX + FILE_LOAD_INTERVAL), 
          TimeUnit.MILLISECONDS);
    }
  }
  
  public void shutdown() {
    timer.stop();
    try {
      client.close();
    } catch (IOException e) {
      LOG.error("Failed to close HttpClient");
    }
  }
  
  public void updateConfig(final AuraMetricsClusterConfig config, 
                           final boolean run_health_checks) {
    LOG.info("Starting Aura health check update.");
    for (final Host host : config.config()) {
      if (!health.containsKey(host)) {
        if (health.putIfAbsent(host, new HealthEntry(host, 
            HealthStatus.UNKNOWN, 0)) != null) {
          LOG.warn("WTF? Lost a race adding a host???: " + host);
        } else if (run_health_checks) {
          // start the load timer
          health_timeouts.put(host, timer.newTimeout(new HealthTask(host), 
              interval, TimeUnit.MILLISECONDS));
          
          if (heartbeats_enabled) {
            heartbeats_timeouts.put(host, timer.newTimeout(
                new HeartbeatTask(host), 
                heartbeats_interval, 
                TimeUnit.MILLISECONDS));
          }
        }
      }
    }
    
    for (final Host host : health.keySet()) {
      if (!config.config().contains(host)) {
        LOG.info("Removing host from config: " + host);
        health.remove(host);
        Timeout timeout = health_timeouts.get(host);
        if (timeout != null) {
          try {
            timeout.cancel();
          } catch (Throwable t) {
            LOG.error("Failed to cancel hosts' health timeout: " + host);
          }
        }
        timeout = heartbeats_timeouts.get(host);
        if (timeout != null) {
          try {
            timeout.cancel();
          } catch (Throwable t) {
            LOG.error("Failed to cancel hosts' namespace timeout: " + host);
          }
        }
      }
    }
    LOG.info("Completed Aura health check update.");
  }
  
  public HealthEntry getEntry(final Host host) {
    return health.get(host);
  }
  
  public long uptimeFudge() {
    return tsdb.getConfig().getLong(KEY_PREFIX + UPTIME_KEY);
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + HEALTH_INTERVAL_KEY)) {
      tsdb.getConfig().register(KEY_PREFIX + HEALTH_INTERVAL_KEY, 1000, true,
          "How often, in milliseconds, to query each hosts' health endpoint.");
    }
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + HEALTH_ENDPOINT_KEY)) {
      tsdb.getConfig().register(KEY_PREFIX + HEALTH_ENDPOINT_KEY, 
          "/api/health", true,
          "The endpoint for the basic health check.");
    }
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + HEARTBEATS_ENDPOINT_KEY)) {
      tsdb.getConfig().register(KEY_PREFIX + HEARTBEATS_ENDPOINT_KEY, 
          "/api/health/*", true,
          "The endpoint for the heartbeats health check.");
    }
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + HEARTBEATS_KEY)) {
      tsdb.getConfig().register(KEY_PREFIX + HEARTBEATS_KEY, true, true,
          "Whether or not to query each host for heartbeats.");
    }
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + HEARTBEATS_INTERVAL_KEY)) {
      tsdb.getConfig().register(KEY_PREFIX + HEARTBEATS_INTERVAL_KEY, 30_000, true,
          "How often, in milliseconds, to query each hosts' namespace "
          + "heartbeat endpoint.");
    }
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + TIME_TAKEN_KEY)) {
      tsdb.getConfig().register(KEY_PREFIX + TIME_TAKEN_KEY, 5_000d, true,
          "How long, in milliseconds, before we consider a host bad "
          + "based on how long the health check took to respond.");
    }
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + SLOW_KEY)) {
      tsdb.getConfig().register(KEY_PREFIX + SLOW_KEY, 1_000d, true,
          "How long, in milliseconds, before we consider a host slow "
          + "based on how long the health check took to respond.");
    }
    if (!tsdb.getConfig().hasProperty(KEY_PREFIX + DROP_KEY)) {
      tsdb.getConfig().register(KEY_PREFIX + DROP_KEY, 5.d, true,
          "How many drops per second before we take a host out of "
          + "rotation.");
      if (!tsdb.getConfig().hasProperty(KEY_PREFIX + LAG_KEY)) {
        tsdb.getConfig().register(KEY_PREFIX + LAG_KEY, 1000, true,
            "How much the lag must grow per second before we take a "
            + "host out of rotation.");
      }
      if (!tsdb.getConfig().hasProperty(KEY_PREFIX + WRITE_KEY)) {
        tsdb.getConfig().register(KEY_PREFIX + WRITE_KEY, 5, true,
            "The minimum number of events written per second before we "
            + "take the host out of rotation.");
      }
      if (!tsdb.getConfig().hasProperty(KEY_PREFIX + UPTIME_KEY)) {
        tsdb.getConfig().register(KEY_PREFIX + UPTIME_KEY, 5 * 60, true,
            "How many seconds to wait before the uptime or first "
            + "heartbeat value befor we consider a host in rotation.");
      }
      if (!tsdb.getConfig().hasProperty(KEY_PREFIX + OVERRIDE_ENABLE_KEY)) {
        tsdb.getConfig().register(KEY_PREFIX + OVERRIDE_ENABLE_KEY, false, true,
            "Whether or not to honor Aura override flags in health check.");
      }
      if (!tsdb.getConfig().hasProperty(KEY_PREFIX + WRITE_PATH)) {
        tsdb.getConfig().register(KEY_PREFIX + WRITE_PATH, null, true,
            "A location where we can write the health status as a JSON file.");
      }
      if (!tsdb.getConfig().hasProperty(KEY_PREFIX + FILE_LOAD_INTERVAL)) {
        tsdb.getConfig().register(KEY_PREFIX + FILE_LOAD_INTERVAL, 10000, true,
            "How often, in milliseconds, to read/write the health status file.");
      }
      if (!tsdb.getConfig().hasProperty(KEY_PREFIX + READ_PATH)) {
        tsdb.getConfig().register(KEY_PREFIX + READ_PATH, null, true,
            "Where to read the status file. Must have a protocol, e.g. "
            + "`file://` or `https://`");
      }
      if (!tsdb.getConfig().hasProperty(KEY_PREFIX + IO_THREADS)) {
        tsdb.getConfig().register(KEY_PREFIX + IO_THREADS, 16, false,
            "How many IO threads to use for the health check client.");
      }
    }
    
    tsdb.getConfig().bind(KEY_PREFIX + HEARTBEATS_KEY, new HeartbeatsCB());
  }

  class HealthTask implements TimerTask {
    private final Host host;
    
    HealthTask(final Host host) {
      this.host = host;
    }
    
    @Override
    public void run(final Timeout ignored) throws Exception {
      long start = DateTime.nanoTime();
      
      try {
        // now we finally fire off the request.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Checking host: " + host);
        }
        final HttpGet request = new HttpGet(host.uri + 
            tsdb.getConfig().getString(KEY_PREFIX + HEALTH_ENDPOINT_KEY));
        class ResponseCallback implements FutureCallback<HttpResponse> {
          
          @Override
          public void cancelled() {
            LOG.error("Run cancelled fetching health for host: " + host 
                + ". We WON'T be rescheduling the health check.");
          }
  
          @Override
          public void completed(final HttpResponse response) {
            try {
              final HealthEntry entry = health.get(host);
              if (entry == null) {
                // We were purged but didn't cancel in time.
                try {
                  EntityUtils.consume(response.getEntity());
                } catch (IOException e) {
                  LOG.error("WTF? Couldn't blackhole the entity??", e);
                }
                return;
              }
              
              double time_taken = DateTime.msFromNanoDiff(DateTime.nanoTime(), start);
              if (LOG.isTraceEnabled()) {
                LOG.trace("Health check [" + request.getURI() + "] took " 
                    + time_taken + "ms to respond.");
              }
              if (response.getStatusLine().getStatusCode() != 200) {
                if (LOG.isTraceEnabled()) {
                  LOG.trace("Non-200 status code [" 
                      + response.getStatusLine().getStatusCode() 
                      + " for Aura: " + request.getURI() + "\n" 
                      + EntityUtils.toString(response.getEntity()));
                }
                allDown(0, "Status code: " + response.getStatusLine().getStatusCode());
                // TODO - parse error
              } else {
                // parse
                final String json = DefaultSharedHttpClient.parseResponse(response, 0, null);
                try {
                  final JsonNode root = JSON.getMapper().readTree(json);
                  
                  JsonNode node = root.get("uptimeSeconds");
                  long uptime = 0;
                  if (node != null && !node.isNull()) {
                    uptime = node.asLong();
                  }
                  
                  // override
                  HealthCheckOverride override = HealthCheckOverride.AUTO;
                  node = root.get("override");
                  if (tsdb.getConfig().getBoolean(KEY_PREFIX + OVERRIDE_ENABLE_KEY) && 
                      node != null && 
                      !node.isNull()) {
                    String ov = node.asText().toLowerCase().trim();
                    if (ov.startsWith("disable")) {
                      override = HealthCheckOverride.DISABLED;
                      entry.setOverride(HealthStatus.DOWN);
                      tsdb.getStatsCollector().setGauge(STATUS_METRIC, 0, 
                          "remote", host.host);
                    } else if (ov.startsWith("enable")) {
                      override = HealthCheckOverride.ENABLED;
                      entry.setOverride(HealthStatus.GOOD);
                      entry.updateStatus(HealthStatus.GOOD, uptime, OK_CAUSE);
                      tsdb.getStatsCollector().setGauge(STATUS_METRIC, 1, 
                          "remote", host.host);
                    } else {
                      entry.setOverride(null);
                    }
                  }
                  
                  // disqualifiers first
                  final int rate_limit = tsdb.getConfig().getInt(KEY_PREFIX + WRITE_KEY);
                  if (rate_limit > 0) {
                    node = root.get("eventReadRate");
                    if (node != null && !node.isNull()) {
                      if (node.asDouble() < rate_limit) {
                        if (LOG.isTraceEnabled()) {
                          LOG.trace("Health check [" + request.getURI() 
                            + "] failed the write rate threshold of [" 
                            + tsdb.getConfig().getInt(KEY_PREFIX + WRITE_KEY) 
                            + "] at " + node.asDouble());
                        }
                        if (override == HealthCheckOverride.AUTO) {
                          allDown(uptime, "Event read rate: " + node.asDouble());
                          return;
                        }
                      }
                    }
                  }
                  
                  final double dropped_limit = tsdb.getConfig().getDouble(KEY_PREFIX + DROP_KEY);
                  if (dropped_limit > 0) {
                    node = root.get("droppedTSRate");
                    if (node != null && !node.isNull()) {
                      if (node.asDouble() > dropped_limit) {
                        if (LOG.isTraceEnabled()) {
                          LOG.trace("Health check [" + request.getURI() 
                            + "] failed the drop rate threshold of [" 
                            + tsdb.getConfig().getDouble(KEY_PREFIX + DROP_KEY) 
                            + "] at " + node.asDouble());
                        }
                        if (override == HealthCheckOverride.AUTO) {
                          allDown(uptime, "Dropped TS rate: " + node.asDouble());
                          return;
                        }
                      }
                    }
                  }
                  
                  final int lag_limit = tsdb.getConfig().getInt(KEY_PREFIX + LAG_KEY);
                  if (lag_limit > 0) {
                    node = root.get("weightedAverageLagRate");
                    if (node != null && !node.isNull()) {
                      if (node.asDouble() > lag_limit) {
                        if (LOG.isTraceEnabled()) {
                          LOG.trace("Health check [" + request.getURI() 
                            + "] failed the Kafka lag threshold of [" 
                            + tsdb.getConfig().getInt(KEY_PREFIX + LAG_KEY) 
                            + "] at " + node.asDouble());
                        }
                        if (override == HealthCheckOverride.AUTO) {
                          allDown(uptime, "Weighted average lag rate: " + node.asDouble());
                          return;
                        }
                      }
                    }
                  }
                  
                  final double time_limit = tsdb.getConfig().getDouble(KEY_PREFIX + TIME_TAKEN_KEY);
                  if (time_limit > 0) {
                    if (time_taken >= time_limit) {
                      if (LOG.isTraceEnabled()) {
                        LOG.trace("Health check [" + request.getURI() 
                          + "] took longer to respond than the threshold of [" 
                          + time_limit 
                          + "] at " + time_taken);
                      }
                      if (override == HealthCheckOverride.AUTO) {
                        allDown(uptime, "Slow due to time taken: " + time_taken);
                        return;
                      }
                    }
                    
                    // so we're not disqualified, let's look at the namespaces
                    if (time_taken > 
                        tsdb.getConfig().getDouble(KEY_PREFIX + SLOW_KEY)) {
                      entry.updateStatus(HealthStatus.SLOW, uptime,
                          "Slow due to time taken: " + time_taken);
                      tsdb.getStatsCollector().setGauge(STATUS_METRIC, 2, 
                          "remote", host.host);
                    } else {
                      entry.updateStatus(HealthStatus.GOOD, uptime, OK_CAUSE);
                      tsdb.getStatsCollector().setGauge(STATUS_METRIC, 1, 
                          "remote", host.host);
                    }
                  }
                  
                } catch (Exception e) {
                  LOG.error("WTF? Failed to parse json: " + json 
                      + " for [" + request.getURI() + "]");
                  allDown(0, e.getMessage());
                  return;
                }
              }
            } catch (Throwable t) {
              LOG.error("Unexpected exception working on response for: " 
                  + request.getURI(), t);
            } finally {
              reschedule();
            }
          }
  
          @Override
          public void failed(final Exception e) {
            try {
              if (health.get(host).status() != HealthStatus.DOWN) {
                LOG.error("Failed to fetch endpoint for host [" + host 
                    + "]. Marking it as down. was: " + health.get(host).status(), e);
                allDown(0, e.getMessage());
              } else {
                if (LOG.isTraceEnabled()) {
                  LOG.trace("Failed to fetch endpoint for host [" + host 
                      + "]. Marking it as down.", e);
                }
                tsdb.getStatsCollector().setGauge(STATUS_METRIC, 0, 
                    "remote", host.host);
              }
              reschedule();
            } catch (Throwable t) {
              LOG.error("Failed to reschedule or mark host out of rotation", t);
            }
          }
          
          void allDown(final long uptime, final String cause) {
            HealthEntry status = health.get(host);
            if (status != null) {
              status.updateStatus(HealthStatus.DOWN, uptime, cause);
              tsdb.getStatsCollector().setGauge(STATUS_METRIC, 0, 
                  "remote", host.host);
            }
            // oops, deleted.
          }
          
        }

        client.execute(request, new ResponseCallback());
      } catch (Throwable t) {
        LOG.error("WTF? Couldn't even send the request for host: " + host, t);
        try {
          reschedule();
        } catch (Throwable t2) {
          LOG.error("Failed to reschedule", t2);
        }
      }
    }
    
    void reschedule() {
      if (health.containsKey(host)) {
        health_timeouts.put(host, timer.newTimeout(this, interval, 
            TimeUnit.MILLISECONDS));
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Oops, our host was removed from the config: " 
              + host + ". Not rescheduling tests.");
        }
      }
    }
  }
  
  class HeartbeatTask implements TimerTask {
    private final Host host;
    
    HeartbeatTask(final Host host) {
      this.host = host;
    }

    @Override
    public void run(final Timeout ignored) throws Exception {
      long start = DateTime.nanoTime();
      
      try {
        // now we finally fire off the request.
        final HttpGet request = new HttpGet(host.uri + 
            tsdb.getConfig().getString(KEY_PREFIX + HEARTBEATS_ENDPOINT_KEY));
        
        class ResponseCallback implements FutureCallback<HttpResponse> {
          
          @Override
          public void cancelled() {
            LOG.error("Run cancelled fetching health for host: " + host 
                + ". We WON'T be rescheduling the health check.");
          }
  
          @Override
          public void completed(final HttpResponse response) {
            try {
              final HealthEntry entry = health.get(host);
              if (entry == null) {
                // We were purged but didn't cancel in time.
                try {
                  EntityUtils.consume(response.getEntity());
                } catch (IOException e) {
                  LOG.error("WTF? Couldn't blackhole the entity??", e);
                }
                return;
              }
            
              double time_taken = DateTime.msFromNanoDiff(DateTime.nanoTime(), start);
              if (LOG.isTraceEnabled()) {
                LOG.trace("Heartbeat [" + request.getURI() + "] took " 
                    + time_taken + "ms to respond.");
              }
              if (response.getStatusLine().getStatusCode() != 200) {
                if (LOG.isTraceEnabled()) {
                  LOG.trace("Non-200 status code [" 
                      + response.getStatusLine().getStatusCode() 
                      + " for Aura: " + request.getURI() + "\n" 
                      + EntityUtils.toString(response.getEntity()));
                }
                allDown(0, "Status code: " + response.getStatusLine().getStatusCode());
                // TODO - parse error
              } else {
                // parse
                final String json = DefaultSharedHttpClient.parseResponse(response, 0, null);
                try {
                  final JsonNode root = JSON.getMapper().readTree(json);
                  
                  final Iterator<Entry<String, JsonNode>> iterator = root.fields();
                  while (iterator.hasNext()) {
                    final Entry<String, JsonNode> namespace = iterator.next();
                    long earliest = 0;
                    long latest = 0;
                    long count = 0;
                    JsonNode node = namespace.getValue().get("earliest");
                    if (node != null && !node.isNull()) {
                      earliest = node.asLong();
                    }
                    
                    node = namespace.getValue().get("latest");
                    if (node != null && !node.isNull()) {
                      latest = node.asLong();
                    }
                    
                    node = namespace.getValue().get("count");
                    if (node != null && !node.isNull()) {
                      count = node.asLong();
                    }
                    
                    entry.updateNamespace(namespace.getKey(), 
                        earliest, latest, count);
                  }
                  
                } catch (Exception e) {
                  LOG.error("WTF? Failed to parse json: " + json 
                      + " for [" + request.getURI() + "]");
                  allDown(0, e.getMessage());
                  return;
                }
              }
            } catch (Throwable t) {
              LOG.error("Unexpected exception working on response", t);
            } finally {
              try {
                reschedule();
              } catch (Throwable t2) {
                LOG.error("Failed to reschedule", t2);
              }
            }
          }
  
          @Override
          public void failed(final Exception e) {
            if (health.get(host).status() != HealthStatus.DOWN) {
              LOG.error("Failed to fetch endpoint for host [" + host 
                  + "]. Marking it as down.", e);
              allDown(0, e.getMessage()); 
            } else {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Failed to fetch endpoint for host [" + host 
                    + "]. Marking it as down.", e);
              }
            }
            reschedule();
          }
          
          void allDown(final long uptime, final String cause) {
            HealthEntry status = health.get(host);
            if (status != null) {
              status.updateStatus(HealthStatus.DOWN, uptime, cause);
            }
            // oops, deleted.
          }
          
        }
        client.execute(request, new ResponseCallback());
      } catch (Throwable t) {
        LOG.error("WTF? Couldn't even send the request for host: " + host, t);
        try {
          reschedule();
        } catch (Throwable t2) {
          LOG.error("Failed to reschedule", t2);
        }
      }
    }
    
    void reschedule() {
      if (health.containsKey(host)) {
        heartbeats_timeouts.put(host, timer.newTimeout(this, heartbeats_interval, 
            TimeUnit.MILLISECONDS));
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Oops, our host was removed from the config: " 
              + host + ". Not rescheduling tests.");
        }
      }
    }    
  }
  
  class HeartbeatsCB implements ConfigurationCallback<Boolean> {

    @Override
    public void update(final String key, final Boolean value) {
      heartbeats_enabled = value;
      
      // TODO - enable or disable
      
    }
    
  }

  /**
   * Writes the state file.
   */
  class WriteFileTask implements TimerTask {

    @Override
    public void run(final Timeout timeout) throws Exception {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        toJson(baos);
        final String path = tsdb.getConfig().getString(KEY_PREFIX + WRITE_PATH);
        Files.write(baos.toByteArray(), new File(path));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wrote state to file: " + path);
        }
      } catch (Exception e) {
        LOG.error("Failed to write state", e);
      } finally {
        tsdb.getMaintenanceTimer().newTimeout(this, 
            tsdb.getConfig().getLong(KEY_PREFIX + FILE_LOAD_INTERVAL), 
            TimeUnit.MILLISECONDS);
      }
    }
    
  }
  
  /**
   * Reads the state file from the given location and updates the health entries
   * instead of checking from this host.
   */
  class ReadStateTask implements TimerTask, FutureCallback<HttpResponse> {

    @Override
    public void run(final Timeout timeout) throws Exception {
      boolean is_file = true;
      try {
        final String json;
        String path = tsdb.getConfig().getString(KEY_PREFIX + READ_PATH);
        if (path.toLowerCase().startsWith("file://")) {
          path = path.replace("file://", "");
          is_file = true;
          CharSource source = Files.asCharSource(new File(path), Const.UTF8_CHARSET);
          json = source.read();
          parseJson(json);
        } else {
          // TODO - for now, assume HTTP
          is_file = false;
          final HttpGet get = new HttpGet(path);
          client.execute(get, this);
        }
      } catch (Exception e) {
        LOG.error("Failed to load state", e);
        is_file = true; // so we reschedule.
      } finally {
        if (is_file) {
          tsdb.getMaintenanceTimer().newTimeout(this, 
              tsdb.getConfig().getLong(KEY_PREFIX + FILE_LOAD_INTERVAL), 
              TimeUnit.MILLISECONDS);
        }
      }
    }

    @Override
    public void completed(final HttpResponse response) {
      try {
        if (response.getStatusLine().getStatusCode() != 200) {
          LOG.warn("Failed to fetch state: " 
              + response.getStatusLine().getStatusCode() + " => " 
              + EntityUtils.toString(response.getEntity()));
          return;
        }
        
        final String json = EntityUtils.toString(response.getEntity());
        parseJson(json);
      } catch (ParseException e) {
        LOG.error("Failed to parse response", e);
      } catch (IOException e) {
        LOG.error("Failed to parse response", e);
      } finally {
        tsdb.getMaintenanceTimer().newTimeout(this, 
            tsdb.getConfig().getLong(KEY_PREFIX + FILE_LOAD_INTERVAL), 
            TimeUnit.MILLISECONDS);
      }
    }

    @Override
    public void failed(final Exception ex) {
      LOG.error("Failed to load health config from server", ex);
      tsdb.getMaintenanceTimer().newTimeout(this, 
          tsdb.getConfig().getLong(KEY_PREFIX + FILE_LOAD_INTERVAL), 
          TimeUnit.MILLISECONDS);
    }

    @Override
    public void cancelled() {
      LOG.warn("Call for health config to server cancelled.");
    }
    
    void parseJson(final String json) {
      try {
        final JsonNode root = JSON.getMapper().readTree(json);
        Host host = new Host(); // re-use object
        for (final JsonNode entry : root) {
          host.host = entry.get("host").asText();
          if (Strings.isNullOrEmpty(host.host)) {
            LOG.error("Null host for entry: " + entry.toString());
            continue;
          }
          
          HealthEntry health_entry = health.get(host);
          if (health_entry == null) {
            LOG.warn("No entry for host: " + host.host);
            continue;
          }
          
          // no locking, just rely on memory ordering as we don't have a 
          // hard synchronization requirement on this data.
          health_entry.uptime = entry.get("uptime").asLong();
          HealthStatus status = HealthStatus.valueOf(entry.get("status").asText());
          if (health_entry.status != status) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Updated aura host status: " + host.host + " from " 
                  + health_entry.status + " to " + status);
            }
            health_entry.status = status;
          }
        }
        
      } catch (IOException e) {
        LOG.error("Failed to process json: " + json);
      }
    }
  }
  
  public void toJson(final OutputStream stream) {
    try {
      final JsonGenerator json = JSON.getFactory().createJsonGenerator(stream);
      json.writeStartArray();
      
      for (final Entry<Host, HealthEntry> entry : health.entrySet()) {
        json.writeStartObject();
        json.writeStringField("host", entry.getKey().host);
        json.writeNumberField("uptime", entry.getValue().uptime());
        json.writeStringField("status", entry.getValue().status().toString());
        json.writeStringField("cause", entry.getValue().cause());
        json.writeArrayFieldStart("namespaces");
        for (final String ns : entry.getKey().namespaces) {
          json.writeString(ns);
        }
        json.writeEndArray();
        json.writeEndObject();
      }
      
      json.writeEndArray();
      json.flush();
      json.close();
    } catch (IOException e) {
      LOG.error("Failed to serialize health info", e);
      throw new IllegalStateException(e);
    }
  }
}
