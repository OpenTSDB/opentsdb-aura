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

package net.opentsdb;

import com.fasterxml.jackson.core.type.TypeReference;
import io.ultrabrew.metrics.util.Strings;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.TSDB;

import java.util.Collections;
import java.util.List;

public class ConfigUtils {

  public static final String CONFIG_PREFIX = "aura.";
  public static final TypeReference<List<String>> STRING_LIST =
          new TypeReference<List<String>>() {};

  public static final String STORAGE_MODE_KEY = "metrics.storage.mode";
  public static final String CONSUMER_MODE_KEY = "metrics.consumer.mode";
  public static final String OPERATING_MODE_KEY = "metrics.operating.mode";
  public static final String NAMESPACES_KEY = "metrics.namespaces";
  public static final String RETENTION_KEY = "metrics.retention";
  public static final String FLUSH_TYPE_KEY = "metrics.flush.type";
  public static final String FLUSH_FREQUENCY_KEY = "metrics.flush.frequency";
  public static final String NAMESPACE_QUERY_KEY = "metrics.query.namespaced";

  public static final String SEGMENT_SIZE_KEY = "metrics.segment.size";
  public static final String MEMORY_USAGE_KEY = "metrics.memory.limit";
  public static final String PURGE_FREQUENCY_KEY = "metrics.purge.frequency";
  public static final String PURGE_BATCH_SIZE_KEY = "metrics.purge.batch.size";
  public static final String SHARD_QUEUE_SIZE_KEY = "metrics.shard.queue.size";
  public static final String SHARDS_KEY = "metrics.shard.count";
  public static final String SHARD_METRIC_TABLE_KEY = "metrics.shard.table.metrics.size";
  public static final String SHARD_TAG_TABLE_KEY = "metrics.shard.table.tags.size";
  public static final String GARBAGE_QUEUE_SIZE_KEY =
          "metrics.shard.segment.collector.queue.size";
  public static final String GARBAGE_DELAY_KEY =
          "metrics.shard.segment.collector.delay";
  public static final String GARBAGE_FREQUENCY_KEY =
          "metrics.shard.segment.collector.frequency";


  // GORILLA BITS - TODO - move this out to an encoding bit.
  public static final String GORILLA_LOSSY_KEY = "metrics.storage.memory.gorilla.lossy.enable";


  private ConfigUtils() {
    // Thou shalt not initialize me!
  }

  public static void registerConfigs(final TSDB tsdb, final String id) {
    final Configuration config = tsdb.getConfig();

    if (!config.hasProperty(configId(id, NAMESPACES_KEY))) {
      config.register(ConfigurationEntrySchema.newBuilder()
              .setKey(configId(id, NAMESPACES_KEY))
              .setType(STRING_LIST)
              .setDefaultValue(Collections.emptyList())
              .setDescription("A list of one or more namespaces to store in this instance")
              .setSource(ConfigUtils.class.getName())
              .build());
    }

    if (!config.hasProperty(configId(id, RETENTION_KEY))) {
      config.register(configId(id, RETENTION_KEY), "24h", false,
              "How long to retain metrics data in memory.");
    }

    // GORILLA
    if (!config.hasProperty(configId(id, GORILLA_LOSSY_KEY))) {
      config.register(configId(id, GORILLA_LOSSY_KEY), false, false,
              "Whether or not to enable lossy compression for Gorilla encoding.");
    }
  }

  public static String configId(final String id, final String suffix) {
    if (Strings.isNullOrEmpty(id)) {
      return CONFIG_PREFIX + suffix;
    }
    return CONFIG_PREFIX + id + "." + suffix;
  }
}
