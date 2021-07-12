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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import net.opentsdb.aura.execution.AuraMetricsClusterConfig.Host;
import net.opentsdb.aura.execution.AuraMetricsHealthChecker.HealthStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HealthEntry {
  private static final Logger LOG = LoggerFactory.getLogger(HealthEntry.class);
  
  private final Host host;
  private final Map<String, NamespaceHealth> namespace_health;
  protected volatile long uptime;
  private volatile HealthStatus override;
  protected volatile HealthStatus status;
  private volatile String cause;
  private volatile boolean recovering;
  private volatile int flap_counter;
  
  HealthEntry(final Host host, final HealthStatus status, final long uptime) {
    this.host = host;
    namespace_health = Maps.newConcurrentMap();
    this.status = status;
    this.uptime = uptime;
  }
  
  public HealthStatus status() {
    if (override != null) {
      return override;
    }
    return status;
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append(host)
        .toString();
  }
  
  public synchronized void setOverride(final HealthStatus override) {
    if (this.override != override) {
      LOG.info("Host override changed from [" + this.override + "] to [" 
          + override + "] for host: " + host);
    }
    recovering = false;
    flap_counter = 0;
    this.override = override;
  }
  
  public boolean isOverriden() {
    return override != null;
  }
  
  public long uptime() {
    return uptime;
  }
  
  public String cause() {
    return cause;
  }
  
  public boolean recovering() {
    return recovering;
  }
  
  public int flapCounter() {
    return flap_counter;
  }
  
  public NamespaceHealth namespaceHealth(final String namespace) {
    return namespace_health.get(namespace);
  }
  
  void purgeNamespace(final String namespace) {
    if (Strings.isNullOrEmpty(namespace)) {
      namespace_health.clear();
    } else {
      namespace_health.remove(namespace);
    }
  }
  
  synchronized void updateStatus(final HealthStatus status, 
                                 final long uptime, 
                                 final String cause) {
    if (override != null) {
      this.status = override;
      flap_counter = 0;
      recovering = false;
      return;
    }
    
    this.uptime = uptime;
    if (this.status != status) {
      if (status == HealthStatus.GOOD) {
        recovering = true;
        if (++flap_counter >= 3) {
          LOG.info("Recovering host [" + host + "] from status [" + this.status 
              + "] flap [" + flap_counter + "] Cause: " + cause);
          this.status = status;
          this.cause = cause;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Good status for aura host [" + host 
                + "] but flap counter at " + flap_counter);
          }
        }
      } else {
        LOG.warn("Marking host [" + host + "] as [" + status + "]. Was [" 
            + this.status + "] Cause: " + cause);
        flap_counter = 0;
        recovering = false;
        this.status = status;
        this.cause = cause;
      }
    }
  }
  
  void updateNamespace(final String namespace, 
                       final long earliest, 
                       final long latest, 
                       final long count) {
    NamespaceHealth health = namespace_health.get(namespace);
    if (health == null) {
      health = new NamespaceHealth();
      namespace_health.put(namespace, health);
    }
    health.update(earliest, latest, count);
  }
}
