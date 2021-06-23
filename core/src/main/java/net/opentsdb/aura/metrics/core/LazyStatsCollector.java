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

package net.opentsdb.aura.metrics.core;

/**
 * Implementor of this interface doesn't instrument till it's asked to. This is done to save some cpu
 * cycles by keeping a local count and pushing the measurement to the underlying instrumentation
 * library in certain intervals.
 */
public interface LazyStatsCollector {

  /** Called on demand to instrument. */
  void collectMetrics();

  /**
   * Dimension used while reporting the metrics.
   *
   * @param tags
   */
  void setTags(String[] tags);
}
