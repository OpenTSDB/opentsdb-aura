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

package net.opentsdb.aura.events;

import com.google.common.base.Strings;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;

public class ConfigUtils {

  public static final String CONFIG_PREFIX = "aura.";

  public static final String STORAGE_DIR_KEY = "events.storage.path";
  public static final String COMMIT_FREQUENCY_KEY = "events.lucene.commit.frequency";

  private ConfigUtils() {
    // Thou shalt not initialize me!
  }

  public static void registerConfigs(final TSDB tsdb, final String id) {
    final Configuration config = tsdb.getConfig();

    if (!config.hasProperty(configId(id, STORAGE_DIR_KEY))) {
      config.register(configId(id, STORAGE_DIR_KEY), "/tmp/opentsdb/events", false,
              "TODO");
    }

    if (!config.hasProperty(configId(id, COMMIT_FREQUENCY_KEY))) {
      config.register(configId(id, COMMIT_FREQUENCY_KEY), 1000, false,
              "TODO");
    }
  }

  public static String configId(final String id, final String suffix) {
    if (Strings.isNullOrEmpty(id)) {
      return CONFIG_PREFIX + suffix;
    }
    return CONFIG_PREFIX + id + "." + suffix;
  }
}
