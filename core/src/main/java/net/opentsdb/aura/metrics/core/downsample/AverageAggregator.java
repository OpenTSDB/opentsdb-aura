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

public class AverageAggregator extends Aggregator {

  public static final byte ID = (byte) 0b1;
  public static final String NAME = "avg";

  private SumAggregator sumAggregator;
  private CountAggregator countAggregator;

  public AverageAggregator(final int numPoints) {
    this(numPoints, null);
  }

  public AverageAggregator(final int numPoints, final Aggregator aggregator) {
    super(0.0, ID, NAME, numPoints, aggregator);
    this.sumAggregator = new SumAggregator(numPoints);
    this.countAggregator = new CountAggregator(numPoints);
  }

  @Override
  protected void doApply(double value) {
    sumAggregator.doApply(value);
    countAggregator.doApply(value);
  }

  @Override
  public void reset() {
    super.reset();
    sumAggregator.reset();
    countAggregator.reset();
  }

  @Override
  public double accumulate(final int index) {
    double sum = sumAggregator.accumulate(index);
    double count = countAggregator.accumulate(index);
    double avg = sum / count;
    value = avg;
    return super.accumulate(index);
  }

  public double[] getSums() {
    return sumAggregator.getValues();
  }

  public double[] getCounts() {
    return countAggregator.getValues();
  }
}
