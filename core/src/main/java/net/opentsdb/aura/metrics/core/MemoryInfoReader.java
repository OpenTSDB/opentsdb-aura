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
 * Platform-specific interface for the memory information of the host on which the Java virtual
 * machine is running
 */
public interface MemoryInfoReader {

  /**
   * @return amount of free physical memory in kilo bytes.
   */
  long getFreePhysicalMemory();

  /**
   * @return total amount of physical memory in kilo bytes.
   */
  long getTotalPhysicalMemory();

  /**
   * @return amount of free swap space in kilo bytes.
   */
  long getFreeSwapMemory();

  /**
   * @return total amount of swap space in kilo bytes.
   */
  long getTotalSwapMemory();

  /**
   * @return percentage of physical memory used in kilo bytes.
   */
  default double getTotalMemoryUsage() {
    long total = getTotalPhysicalMemory();
    long free = getFreePhysicalMemory();
    return ((double) total - free) / (double) total * 100;
  }
}
