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

import java.util.Arrays;
import java.util.Iterator;

public abstract class Aggregator {

  private final double identity;
  private final double[] values;
  protected double value;
  protected byte id;
  protected String name;

  private final Aggregator previous;
  private Aggregator next;

  public static AggregatorBuilder newBuilder() {
    return new AggregatorBuilder();
  }

  public Aggregator(final double identity, final byte id, final String name) {
    this(identity, id, name, null);
  }

  public Aggregator(
      final double identity, final byte id, final String name, final Aggregator previous) {
    this.identity = identity;
    this.previous = previous;
    this.values = new double[240];
    this.id = id;
    this.name = name;
    if (previous != null) {
      this.id |= previous.id;
      this.name = previous.name + "-" + name;
      previous.next = this;
    }
  }

  public void reset() {
    if (null != previous) {
      previous.reset();
    }
    value = identity;
    Arrays.fill(values, Double.NaN);
  }

  public void apply(double value) {
    if (null != previous) {
      previous.apply(value);
    }
    doApply(value);
  }

  public double accumulate(final int index) {
    if (null != previous) {
      previous.accumulate(index);
    }
    this.values[index] = value;
    double agg = value;
    this.value = identity;
    return agg;
  }

  public byte getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  protected abstract void doApply(double value);

  public double[] getValues() {
    return values;
  }

  private Aggregator head() {
    if (null != previous) {
      return previous.head();
    }
    return this;
  }

  public Iterator<double[]> iterator() {

    return new Iterator<>() {
      Aggregator head = head();

      @Override
      public boolean hasNext() {
        return head != null;
      }

      @Override
      public double[] next() {
        double[] values = head.values;
        head = head.next;
        return values;
      }
    };
  }

  public static class AggregatorBuilder {

    private Aggregator aggregator;

    public AggregatorBuilder avg() {
      aggregator = new AverageAggregator(aggregator);
      return this;
    }

    public AggregatorBuilder count() {
      aggregator = new CountAggregator(aggregator);
      return this;
    }

    public AggregatorBuilder sum() {
      aggregator = new SumAggregator(aggregator);
      return this;
    }

    public AggregatorBuilder min() {
      aggregator = new MinAggregator(aggregator);
      return this;
    }

    public AggregatorBuilder max() {
      aggregator = new MaxAggregator(aggregator);
      return this;
    }

    public AggregatorBuilder sumOfSquares() {
      aggregator = new SumOfSquareAggregator(aggregator);
      return this;
    }

    public Aggregator build() {
      return aggregator;
    }
  }
}
