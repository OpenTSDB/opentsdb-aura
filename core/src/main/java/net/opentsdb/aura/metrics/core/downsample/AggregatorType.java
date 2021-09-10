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
    public Aggregator create(int intervalCount, Aggregator aggregator) {
      return new AverageAggregator(intervalCount, aggregator);
    }

    @Override
    public int getOrdinal() {
      return AverageAggregator.ORDINAL;
    }

    @Override
    public byte getId() {
      return AverageAggregator.ID;
    }
  },
  sum {
    @Override
    public Aggregator create(int intervalCount, Aggregator aggregator) {
      return new SumAggregator(intervalCount, aggregator);
    }

    @Override
    public int getOrdinal() {
      return SumAggregator.ORDINAL;
    }

    @Override
    public byte getId() {
      return SumAggregator.ID;
    }
  },
  count {
    @Override
    public Aggregator create(int intervalCount, Aggregator aggregator) {
      return new CountAggregator(intervalCount, aggregator);
    }

    @Override
    public int getOrdinal() {
      return CountAggregator.ORDINAL;
    }

    @Override
    public byte getId() {
      return CountAggregator.ID;
    }
  },
  min {
    @Override
    public Aggregator create(int intervalCount, Aggregator aggregator) {
      return new MinAggregator(intervalCount, aggregator);
    }

    @Override
    public int getOrdinal() {
      return MinAggregator.ORDINAL;
    }

    @Override
    public byte getId() {
      return MinAggregator.ID;
    }
  },
  max {
    @Override
    public Aggregator create(int intervalCount, Aggregator aggregator) {
      return new MaxAggregator(intervalCount, aggregator);
    }

    @Override
    public int getOrdinal() {
      return MaxAggregator.ORDINAL;
    }

    @Override
    public byte getId() {
      return MaxAggregator.ID;
    }
  },
  sumofsquare {
    @Override
    public Aggregator create(int intervalCount, Aggregator aggregator) {
      return new SumOfSquareAggregator(intervalCount, aggregator);
    }

    @Override
    public int getOrdinal() {
      return SumOfSquareAggregator.ORDINAL;
    }

    @Override
    public byte getId() {
      return SumOfSquareAggregator.ID;
    }
  };

  public abstract Aggregator create(int intervalCount, Aggregator aggregator);

  public abstract int getOrdinal();

  public abstract byte getId();

  public static AggregatorType getByOrdinal(final int ordinal) {
    return values()[ordinal - 1];
  }
}
