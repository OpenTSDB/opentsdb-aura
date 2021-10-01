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

import net.opentsdb.core.TSDBPlugin;

import java.util.List;
import java.util.Map;

/**
 * TEMP! This is a mock discovery service.
 */
public interface MockDiscoveryService extends TSDBPlugin {

  interface ShardEndPoint {
    String getHost();

    int getPort();
  }

  Map<String, List<ShardEndPoint>> getEndpoints(String namespace);

  /**
   *
   * @param namespace
   * @param start Start of the query in unix epoch seconds
   * @param end End of the query in unix epoch seconds
   * @return A map of service name (Myst, Aura, etc.) to endpoints.
   */
  Map<String, List<ShardEndPoint>> getEndpoints(String namespace, long start, long end);

}
