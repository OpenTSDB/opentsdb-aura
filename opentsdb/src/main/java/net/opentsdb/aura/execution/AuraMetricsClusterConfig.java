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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = AuraMetricsClusterConfig.Builder.class)
public class AuraMetricsClusterConfig {
  private static final Logger LOG = LoggerFactory.getLogger(AuraMetricsClusterConfig.class);
  protected final List<Host> raw_config;
  
  /** colo, namespace, hosts */
  protected final Map<String, Map<String, Hosts>> hosts;
  
  protected AuraMetricsClusterConfig(final Builder builder) {
    raw_config = builder.hostMap;
    
    try {
      // rebuild into something we can query nicely.
      hosts = Maps.newHashMapWithExpectedSize(2);
      final Pattern regexp = Pattern.compile("yms\\.(\\w\\w)\\d\\.");
      for (final Host host : raw_config) {
        Matcher match = regexp.matcher(host.host);
        if (!match.find()) {
          throw new IllegalArgumentException("Unable to parse hostname "
              + "from host: " + host.host);
        }
        final String colo = match.group(1).toLowerCase();
        Map<String, Hosts> extant = hosts.get(colo);
        if (extant == null) {
          extant = Maps.newHashMap();
          hosts.put(colo, extant);
        }
        
        for (final String namespace : host.namespaces) {
          Hosts host_list = extant.get(namespace);
          if (host_list == null) {
            host_list = new Hosts(Lists.newArrayList(host));
            extant.put(namespace, host_list);
          } else {
            host_list.hosts.add(host);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Whoops, couldn't parse the config.", e);
      throw e;
    }
  }
  
  public Hosts getHosts(final String colo, final String namespace) {
    final Map<String, Hosts> namespaces = hosts.get(colo.toLowerCase());
    if (namespaces == null) {
      return null;
    }
    return namespaces.get(namespace);
  }
  
  List<Host> config() {
    return raw_config;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    @JsonProperty
    private List<Host> hostMap;
    
    public Builder setHosts(final List<Host> hosts) {
      hostMap = hosts;
      return this;
    }
    
    public Builder addHost(final Host host) {
      if (hostMap == null) {
        hostMap = Lists.newArrayList();
      }
      hostMap.add(host);
      return this;
    }
    
    public AuraMetricsClusterConfig build() {
      return new AuraMetricsClusterConfig(this);
    }
    
  }
  
  public static class Hosts {
    public volatile int idx;
    public List<Host> hosts;
    public long retention = Long.MAX_VALUE;
    
    Hosts(final List<Host> hosts) {
      this.hosts = hosts;
      
      try {
        for (int i = 0; i < hosts.size(); i++) {
          if (!Strings.isNullOrEmpty(hosts.get(i).retention)) {
            long seconds = DateTime.parseDuration(hosts.get(i).retention) / 1000;
            // NOTE: We pick the smaller non-null retention on purpose.
            if (seconds < retention) {
              retention = seconds;
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Whoops, failed to parse host entry", e);
        throw e;
      }
    }
    
    Host nextHost() {
      int idx = 0;
      synchronized (this) {
        if (++this.idx >= hosts.size()) {
          this.idx = 0;
        }
        idx = this.idx;
      }
      return hosts.get(idx);
    }
    
    List<Host> hosts() {
      return hosts;
    }
    
    public long retention() {
      return retention;
    }
    
    @Override
    public String toString() {
      return hosts.toString();
    }
  }

  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Host {
    public String host;
    public String uri;
    public String retention;
    public List<String> namespaces;

    public Host() {

    }

    public Host(final String host,
                final String uri,
                final String retention,
                final List<String> namespaces) {
      this.host = host;
      this.uri = uri;
      this.retention = retention;
      this.namespaces = namespaces;
    }

    @Override
    public int hashCode() {
      return host.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Host)) {
        return false;
      }
      return ((Host) obj).host.equals(host);
    }

    @Override
    public String toString() {
      return new StringBuilder()
          .append("host=")
          .append(host)
          .append(", uri=")
          .append(uri)
          .append(", retention=")
          .append(retention)
          .append(", namespaces=")
          .append(namespaces)
          .toString();
    }
  }
}
