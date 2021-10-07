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

public class SumAggregator extends Aggregator {

  public static final int ORDINAL = 2;
  public static final byte ID = (byte) 0b10;
  public static final String NAME = "sum";

  public SumAggregator(final int numPoints) {
    this(numPoints, null);
  }

  public SumAggregator(final int numPoints, final Aggregator aggregator) {
    super(0.0, ORDINAL, ID, NAME, numPoints, aggregator);
  }

  @Override
  protected void doApply(double value) {
    this.value += value;
  }
}
