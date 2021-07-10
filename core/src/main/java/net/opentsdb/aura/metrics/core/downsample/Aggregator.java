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

  protected final double identity;
  protected double value;
  protected byte id;
  protected String name;
  protected final double[] values;

  private final Aggregator previous;
  private Aggregator next;

  private AggregatorIterator iterator;

  public static AggregatorBuilder newBuilder() {
    return new AggregatorBuilder();
  }

  public Aggregator(final double identity, final byte id, final String name, final int numPoints) {
    this(identity, id, name, numPoints, null);
  }

  public Aggregator(
      final double identity,
      final byte id,
      final String name,
      final int numPoints,
      final Aggregator previous) {
    this.identity = identity;
    this.previous = previous;
    this.values = new double[numPoints];
    this.id = id;
    this.name = name;
    if (previous != null) {
      if ((id & previous.id) != 0) {
        throw new IllegalArgumentException("Duplicate aggregator found for: " + name);
      }
      this.id |= previous.id;
      this.name = previous.name + "-" + name;
      previous.next = this;
    }
  }

  public void reset() {
    if (null != previous) {
      previous.reset();
    }
    doReset();
  }

  protected void doReset() {
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

    if (iterator == null) {
      iterator = new AggregatorIterator();
    }
    iterator.reset();
    return iterator;
  }

  private class AggregatorIterator implements Iterator<double[]> {

    Aggregator current;
    Aggregator head;

    public AggregatorIterator() {
      this.head = head();
      reset();
    }

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public double[] next() {
      double[] values = current.values;
      current = current.next;
      return values;
    }

    void reset() {
      current = head;
    }
  }

  public static class AggregatorBuilder {

    private Aggregator aggregator;

    public AggregatorBuilder avg(final int numPoints) {
      aggregator = new AverageAggregator(numPoints, aggregator);
      return this;
    }

    public AggregatorBuilder count(final int numPoints) {
      aggregator = new CountAggregator(numPoints, aggregator);
      return this;
    }

    public AggregatorBuilder sum(final int numPoints) {
      aggregator = new SumAggregator(numPoints, aggregator);
      return this;
    }

    public AggregatorBuilder min(final int numPoints) {
      aggregator = new MinAggregator(numPoints, aggregator);
      return this;
    }

    public AggregatorBuilder max(final int numPoints) {
      aggregator = new MaxAggregator(numPoints, aggregator);
      return this;
    }

    public AggregatorBuilder sumOfSquares(final int numPoints) {
      aggregator = new SumOfSquareAggregator(numPoints, aggregator);
      return this;
    }

    public Aggregator build() {
      return aggregator;
    }
  }
}
