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
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.exceptions.QueryDownstreamException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AggregatorFinishedTS implements TimeSeries {

  private final AerospikeBatchQueryNode queryNode;
  private final MetaTimeSeriesQueryResult.GroupResult groupResult;
  private NumericArrayAggregator aggregator;
  protected TimeSeriesId id;
  protected boolean has_next;

  public AggregatorFinishedTS(final AerospikeBatchQueryNode queryNode,
                              final MetaTimeSeriesQueryResult.GroupResult groupResult,
                              final NumericArrayAggregator aggregator) {
    this.queryNode = queryNode;
    this.groupResult = groupResult;
    this.aggregator = aggregator;
    if (aggregator != null) {
      has_next = aggregator.end() > aggregator.offset();
    }
  }

  @Override
  public TimeSeriesId id() {
    if (id == null) {
      synchronized (this) {
        if (id == null) {
          id = new TSID();
        }
      }
    }
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
          final TypeToken<? extends TimeSeriesDataType> type) {
    if (type != NumericArrayType.TYPE)  {
      return Optional.empty();
    }
    return Optional.of(new AggregatorIterator());
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators
            = Lists.newArrayList();
    iterators.add(new AggregatorIterator());
    return iterators;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return NumericArrayType.SINGLE_LIST;
  }

  @Override
  public void close() {
    try {
      if (aggregator != null) {
        aggregator.close();
        aggregator = null;
      }
    } catch (IOException e) {
      queryNode.onError(e);
    }
  }

  public void combine(AggregatorFinishedTS value) {
    if (value.aggregator.end() > value.aggregator.offset()) {
      aggregator.combine(value.aggregator);
      has_next = true;
    }
  }

  class AggregatorIterator implements TypedTimeSeriesIterator<NumericArrayType>,
          TimeSeriesValue<NumericArrayType> {

    protected boolean read = !has_next;

    @Override
    public TypeToken<NumericArrayType> getType() {
      return NumericArrayType.TYPE;
    }

    @Override
    public void close() throws IOException {
      // no-op, let the time series close release the aggregator.
    }

    @Override
    public boolean hasNext() {
      return !read;
    }

    @Override
    public TimeSeriesValue<NumericArrayType> next() {
      read = true;
      return this;
    }

    @Override
    public TimeStamp timestamp() {
      return queryNode.queryResult().timeSpecification().start();
    }

    @Override
    public NumericArrayType value() {
      return aggregator;
    }

    @Override
    public TypeToken<NumericArrayType> type() {
      return NumericArrayType.TYPE;
    }
  }

  class TSID implements TimeSeriesStringId {

    private Map<String, String> tags;

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
      return queryNode.metricString();
    }

    @Override
    public Map<String, String> tags() {
      if (tags != null) {
        return tags;
      }

      tags = Maps.newHashMap();
      try {
        MetaTimeSeriesQueryResult.GroupResult.TagHashes hashes = groupResult.tagHashes();
        String[] tagKeys = queryNode.gbKeys();
        if (tagKeys != null) {
          if (hashes.size() != tagKeys.length) {
            queryNode.onError(new QueryDownstreamException(
                    "Bad group by tags hash length. Got " + hashes.size()
                            + " when we need: " + tagKeys.length));
            return tags;
          }
          for (int i = 0; i < tagKeys.length; i++) {
            long hash = hashes.next();
            tags.put(tagKeys[i], queryNode.getStringForHash(hash));
          }
        }
      } catch (Throwable t) {
        queryNode.onError(t);
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
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean encoded() {
      return false;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> type() {
      return Const.TS_STRING_ID;
    }

    @Override
    public long buildHashCode() {
      throw new UnsupportedOperationException("TODO");
    }
  }
}