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

public class ShardConfig {

  public String namespace;

  public boolean lossy = true;
  public int segmentBlockSizeBytes = 256;

  public int[] shardIds;
  public int shardCount = 10;
  public int queueSize = 20_000;
  public int segmentSizeHour = 2;
  public int[] segmentStartTimes;
  public int retentionHour = 24;
  public double memoryUsageLimitPct = 90;
  public int metaStoreCapacity = 1_000_000;


  public int tagTableSize = 3_000_000;
  public int metricTableSize = 400;
  public int timeSeriesTableSize = 3_000_000;
  public int garbageQSize = 2_000_000;

  public int segmentCollectionDelayMinutes = 15;
  public int segmentCollectionFrequencySeconds = 10;
  public int purgeFrequencyMinutes = 120;
  public int metaPurgeBatchSize = 4000;

  public boolean metaQueryEnabled;

  public int getSegmentWallClockHour() {
    if (null != segmentStartTimes && segmentStartTimes.length > 0) {
      return segmentStartTimes[0];
    }
    return -1;
  }
}
