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

public abstract class Aggregator {

  protected final double identity;
  protected double value;
  protected byte id;
  protected String name;
  protected byte compositeId;
  protected String compositeName;
  protected final double[] values;
  protected int aggregatorCount;

  private final Aggregator previous;
  protected Aggregator next;

  private AggregatorIteratorImpl iterator;

  public static AggregatorBuilder newBuilder(final int intervalCount) {
    return new AggregatorBuilder(intervalCount);
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
    this.compositeId = id;
    this.name = name;
    this.compositeName = name;
    this.aggregatorCount = 1;
    if (previous != null) {
      if ((id & previous.compositeId) != 0) {
        throw new IllegalArgumentException("Duplicate aggregator found for: " + name);
      }
      this.compositeId |= previous.compositeId;
      this.compositeName = previous.compositeName + "-" + name;
      previous.next = this;
      aggregatorCount += previous.aggregatorCount;
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
    return compositeId;
  }

  public String getName() {
    return compositeName;
  }

  protected abstract void doApply(double value);

  public double[] getValues() {
    return values;
  }

  protected Aggregator head() {
    if (null != previous) {
      return previous.head();
    }
    return this;
  }

  public AggregatorIterator<double[]> iterator() {

    if (iterator == null) {
      iterator = new AggregatorIteratorImpl();
    }
    iterator.reset();
    return iterator;
  }

  public int getAggCount() {
    return aggregatorCount;
  }

  public class AggregatorIteratorImpl implements AggregatorIterator<double[]> {

    Aggregator current;
    Aggregator head;

    byte aggId;
    String aggName;

    public AggregatorIteratorImpl() {
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
      aggId = current.id;
      aggName = current.name;
      current = current.next;
      return values;
    }

    @Override
    public void reset() {
      current = head;
      aggId = 0;
      aggName = null;
    }

    @Override
    public byte aggID() {
      return aggId;
    }

    @Override
    public String aggName() {
      return aggName;
    }
  }

  public static class AggregatorBuilder {

    private int intervalCount;

    public AggregatorBuilder(final int intervalCount) {
      this.intervalCount = intervalCount;
    }

    private Aggregator aggregator;

    public AggregatorBuilder forType(final AggregatorType type) {
      aggregator = type.create(intervalCount, aggregator);
      return this;
    }

    public AggregatorBuilder forType(final String type) {
      return forType(AggregatorType.valueOf(type.toLowerCase()));
    }

    public AggregatorBuilder avg() {
      aggregator = new AverageAggregator(intervalCount, aggregator);
      return this;
    }

    public AggregatorBuilder count() {
      aggregator = new CountAggregator(intervalCount, aggregator);
      return this;
    }

    public AggregatorBuilder sum() {
      aggregator = new SumAggregator(intervalCount, aggregator);
      return this;
    }

    public AggregatorBuilder min() {
      aggregator = new MinAggregator(intervalCount, aggregator);
      return this;
    }

    public AggregatorBuilder max() {
      aggregator = new MaxAggregator(intervalCount, aggregator);
      return this;
    }

    public AggregatorBuilder sumOfSquares() {
      aggregator = new SumOfSquareAggregator(intervalCount, aggregator);
      return this;
    }

    public Aggregator build() {
      return aggregator;
    }
  }
}
