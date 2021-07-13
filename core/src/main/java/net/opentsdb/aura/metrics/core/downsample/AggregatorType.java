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

public enum AggregatorType {
  avg {
    @Override
    public Aggregator create(int numPoints, Aggregator aggregator) {
      return new AverageAggregator(numPoints, aggregator);
    }
  },
  sum {
    @Override
    public Aggregator create(int numPoints, Aggregator aggregator) {
      return new SumAggregator(numPoints, aggregator);
    }
  },
  count {
    @Override
    public Aggregator create(int numPoints, Aggregator aggregator) {
      return new CountAggregator(numPoints, aggregator);
    }
  },
  min {
    @Override
    public Aggregator create(int numPoints, Aggregator aggregator) {
      return new MinAggregator(numPoints, aggregator);
    }
  },
  max {
    @Override
    public Aggregator create(int numPoints, Aggregator aggregator) {
      return new MaxAggregator(numPoints, aggregator);
    }
  },
  sumofsquare {
    @Override
    public Aggregator create(int numPoints, Aggregator aggregator) {
      return new SumOfSquareAggregator(numPoints, aggregator);
    }
  };

  public abstract Aggregator create(int numPoints, Aggregator aggregator);
}
