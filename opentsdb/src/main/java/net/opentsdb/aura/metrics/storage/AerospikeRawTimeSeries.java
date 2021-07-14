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

package net.opentsdb.aura.metrics.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import net.opentsdb.aura.metrics.core.LongTermStorage;
import net.opentsdb.aura.metrics.core.RawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AerospikeRawTimeSeries implements TimeSeries {
  private static Logger logger = LoggerFactory.getLogger(AerospikeRawTimeSeries.class);

  AerospikeQueryResult result;
  MetaTimeSeriesQueryResult.GroupResult gr;
  TimeSeriesId id;

  public AerospikeRawTimeSeries(AerospikeQueryResult result, MetaTimeSeriesQueryResult.GroupResult gr) {
    this.result = result;
    this.gr = gr;
  }

  @Override
  public TimeSeriesId id() {
    if (id == null) {
      id = new TSID();
    }
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(TypeToken<? extends TimeSeriesDataType> typeToken) {
    if (typeToken == NumericType.TYPE) {
      return Optional.of(new LocalIterator());
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    List<TypedTimeSeriesIterator<?>> list = Lists.newArrayList();
    list.add(new LocalIterator());
    return list;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return NumericType.SINGLE_LIST;
  }

  @Override
  public void close() {
    // no-op for now.
  }

  class TSID implements TimeSeriesStringId {

    Map<String, String> tags;

    @Override
    public String alias() {
      return null;
    }

    @Override
    public String namespace() {
      return null;
    }

    @Override
    public String metric() {
      return result.metricString();
    }

    @Override
    public Map<String, String> tags() {
      if (tags != null) {
        return tags;
      }

      tags = Maps.newHashMap();
      try {
        MetaTimeSeriesQueryResult.GroupResult.TagHashes hashes = gr.tagHashes();
        String key = null;
        for (int i = 0; i < hashes.size(); i++) {
          long hash = hashes.next();
          if (i % 2 == 0) {
            key = result.metaResult().getStringForHash(hash);
          } else {
            tags.put(key, result.metaResult().getStringForHash(hash));
          }
        }
      } catch (Throwable t) {
        logger.error("Failed tag fetch", t);
      }
      return tags;
    }

    @Override
    public String getTagValue(String s) {
      return tags().get(s);
    }

    @Override
    public List<String> aggregatedTags() {
      return Collections.emptyList();
    }

    @Override
    public List<String> disjointTags() {
      return Collections.emptyList();
    }

    @Override
    public Set<String> uniqueIds() {
      return null;
    }

    @Override
    public long hits() {
      return 0;
    }

    @Override
    public int compareTo(TimeSeriesStringId o) {
      return 0;
    }

    @Override
    public boolean encoded() {
      return false;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> type() {
      return null;
    }

    @Override
    public long buildHashCode() {
      return 0;
    }
  }

  class LocalIterator implements TypedTimeSeriesIterator<NumericType> {

    MutableNumericValue dp;
    MutableNumericValue next;
    boolean has_next;
    LongTermStorage.Records<RawTimeSeriesEncoder> records;
    double[] values;
    int idx;
    int base_timestamp;
    int queryEnd = (int) result.source().pipelineContext().query().endTime().epoch();
    int queryStart = (int) result.source().pipelineContext().query().startTime().epoch();

    LocalIterator() {
      records = ((AerospikeQueryNode) result.source()).asClient()
              .read(gr.getHash(0),
                      ((AerospikeQueryNode) result.source()).getSegmentsStart(),
                      ((AerospikeQueryNode) result.source()).getSegmentsEnd());
      dp = new MutableNumericValue();
      next = new MutableNumericValue();
      values = ((AerospikeQueryNode) result.source()).getSegmentReadArray();
      Arrays.fill(values, Double.NaN);
      has_next = nextEncoder();
      advance();
    }

    @Override
    public TypeToken<NumericType> getType() {
      return NumericType.TYPE;
    }

    @Override
    public void close() throws IOException {
      // no-op for now
    }

    @Override
    public boolean hasNext() {
      return has_next;
    }

    @Override
    public TimeSeriesValue<NumericType> next() {
      dp.reset(next);
      advance();
      return dp;
    }

    int recordsRead = 0;
    int nulls;

    boolean nextEncoder() {
      while (records.hasNext()) {
        RawTimeSeriesEncoder encoder = records.next();
        if (encoder == null) {
          // nothing for this segment slice
          nulls++;
          continue;
        }

        int read = encoder.readAndDedupe(values);
        if (read <= 0) {
          // shouldn't happen but...
          continue;
        }

        // had an encoder so hand it back to advance() to find valid data.
        recordsRead++;
        base_timestamp = encoder.getSegmentTime();
        return true;
      }

      has_next = false;
      return false;
    }

    void advance() {
      while (true) {
        for (; idx < values.length; idx++) {
          if (Double.isNaN(values[idx])) {
            continue;
          }

          // got a value!
          int ts = base_timestamp + idx;
          if (ts < queryStart) {
            continue;
          }

          if (ts >= queryEnd) {
            has_next = false;
            return;
          }

          // good value!
          next.timestamp().updateEpoch(ts);
          next.resetValue(values[idx]);
          idx++;
          return;
        }
        // next segment
        idx = 0;

        if (!nextEncoder()) {
          has_next = false;
          return;
        }
      }
    }
  }
}