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

package net.opentsdb.aura.metrics.core.gorilla;

import io.ultrabrew.metrics.MetricRegistry;
import net.opentsdb.aura.metrics.core.OffHeapSegment;
import net.opentsdb.aura.metrics.core.downsample.DownSampledSegment;

public class OffHeapDownSampledGorillaSegment extends OffHeapSegment
    implements DownSampledSegment, GorillaSegment {

  protected static final int INTERVAL_BYTE_INDEX = 22;
  protected static final int AGG_BYTE_INDEX = 23;
  protected static final int HEADER_SIZE_BYTE = 24;

  public OffHeapDownSampledGorillaSegment(
      final int dataBlockSizeBytes, final MetricRegistry metricRegistry) {
    super(dataBlockSizeBytes);
  }

  @Override
  public void setInterval(byte interval) {
    header.setByte(INTERVAL_BYTE_INDEX, interval);
  }

  @Override
  public byte getInterval() {
    return header.getByte(INTERVAL_BYTE_INDEX);
  }

  @Override
  public void setAggs(byte aggId) {
    header.setByte(AGG_BYTE_INDEX, aggId);
  }

  @Override
  public byte getAggs() {
    return header.getByte(AGG_BYTE_INDEX);
  }

  @Override
  protected int headerSizeBytes() {
    return HEADER_SIZE_BYTE;
  }

  @Override
  public boolean isDirty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasDupesOrOutOfOrderData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markFlushed() {
    throw new UnsupportedOperationException();
  }
}
