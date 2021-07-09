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

package net.opentsdb.aura.metrics.core.downsample;

import java.util.Iterator;

public class DownSampler {

  private final int interval;
  private int numPoints;
  private final Aggregator aggregator;

  public DownSampler(final int interval, final int numPoints, final Aggregator aggregator) {
    this.interval = interval;
    this.numPoints = numPoints;
    this.aggregator = aggregator;
  }

  public void apply(final double[] rawValues) {
    aggregator.reset();
    int offset = 0;
    boolean intervalHasValue = false;
    int lastIndex = rawValues.length - 1;
    for (int i = 0; i <= lastIndex; i++) {
      double value = rawValues[i];
      if (!Double.isNaN(value)) {
        aggregator.apply(value);
        if (!intervalHasValue) {
          intervalHasValue = true;
        }
      }

      offset++;
      if ((offset == interval || i == lastIndex) && intervalHasValue) {
        int index = i / interval;
        aggregator.accumulate(index);
        offset = 0;
        intervalHasValue = false;
      }
    }
  }

  public Iterator<double[]> iterator() {
    return aggregator.iterator();
  }

  public byte getAggId() {
    return aggregator.getId();
  }
}
