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
package net.opentsdb.aura.metrics.system;

import net.opentsdb.aura.metrics.core.MemoryInfoReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * The memory information reader for RHEL 6. It uses a background thread to periodically reads the
 * /proc/meminfo file and caches the memory information.
 */
public class Rhel6MemoryInfoReader implements MemoryInfoReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(Rhel6MemoryInfoReader.class);
  private static final String DEFAULT_FILE_PATH = "/proc/meminfo";

  private final int intervalMillis;
  private final File file;
  private Thread readThread;
  private volatile MemoryInfo memoryInfo;

  /**
   * Creates a memory reader that reads /proc/meminfo file at a given interval
   *
   * @param intervalMillis interval to refresh the memory info
   */
  public Rhel6MemoryInfoReader(final int intervalMillis) {
    this(intervalMillis, DEFAULT_FILE_PATH);
  }

  /**
   * Creates a memory reader that reads a custom file at a given interval. Exposed for testing.
   *
   * @param intervalMillis interval to refresh the memory info
   * @param filePath file to read the memory info from
   */
  Rhel6MemoryInfoReader(final int intervalMillis, final String filePath) {
    this.intervalMillis = intervalMillis;
    this.file = new File(filePath);
    assert this.file.exists() : "File not found: " + filePath;
    read();
    start();
  }

  private void start() {
    this.readThread = new Thread(this::run, "MemoryInfoReader");
    this.readThread.setDaemon(true);
    this.readThread.start();
  }

  private void run() {
    while (true) {
      long delay = calculateDelay(intervalMillis, System.currentTimeMillis());
      try {
        Thread.sleep(delay);
        read();
      } catch (InterruptedException ignored) {
        // Should not happen, just looping again is fine
      }
    }
  }

  void read() {
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      MemoryInfo memoryInfo = new MemoryInfo();
      while ((line = br.readLine()) != null) {
        String[] split = line.split("\\s+");
        if (split.length >= 2) {
          String label = split[0];
          long valueKB = Long.parseLong(split[1]);

          switch (label) {
            case "MemTotal:":
              memoryInfo.total = valueKB;
              break;
            case "MemFree:":
              memoryInfo.free = valueKB;
              break;
            case "Buffers:":
              memoryInfo.buffer = valueKB;
              break;
            case "Cached:":
              memoryInfo.cached = valueKB;
              break;
            case "SwapTotal:":
              memoryInfo.swapTotal = valueKB;
              break;
            case "SwapFree:":
              memoryInfo.swapFree = valueKB;
              break;
            default:
              //ignore
          }

        }
      }
      this.memoryInfo = memoryInfo;
    } catch (IOException e) {
      LOGGER.error("Error reading memory info", e);
    }
  }

  @Override
  public long getFreePhysicalMemory() {
    return memoryInfo.free + memoryInfo.cached + memoryInfo.buffer;
  }

  @Override
  public long getTotalPhysicalMemory() {
    return memoryInfo.total;
  }

  @Override
  public long getTotalSwapMemory() {
    return memoryInfo.swapTotal;
  }

  @Override
  public long getFreeSwapMemory() {
    return memoryInfo.swapFree;
  }

  private static long calculateDelay(final long interval, final long currentTime) {
    return interval - (currentTime % interval);
  }

  private static class MemoryInfo {

    long total;
    long free;
    long buffer;
    long cached;
    long swapTotal;
    long swapFree;
  }

}
