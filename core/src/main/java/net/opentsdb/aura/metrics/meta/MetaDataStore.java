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

package net.opentsdb.aura.metrics.meta;

import net.opentsdb.aura.metrics.core.data.InSufficientBufferLengthException;
import net.opentsdb.data.LowLevelMetricData;

public interface MetaDataStore {

  void add(LowLevelMetricData.HashedLowLevelMetricData event);

  void addMetric(LowLevelMetricData.HashedLowLevelMetricData event);

  SharedMetaResult queryMeta(MetaQuery query);

  int search(Query query, String metric, long[] buffer) throws InSufficientArrayLengthException;

  int size();

  boolean remove(long docId, byte[] tagBuffer, int offset, int length);

  void remove(long[] docIdBatch, int batchSize, byte[] byteBuffer) throws InSufficientBufferLengthException;

  class InSufficientArrayLengthException extends Exception {
    private static final long serialVersionUID = 5147813747196460363L;
    private final int required;
    private final int found;

    public InSufficientArrayLengthException(final int required, final int found) {
      this.required = required;
      this.found = found;
    }

    public int getRequired() {
      return required;
    }

    public int getFound() {
      return found;
    }
  }
}
