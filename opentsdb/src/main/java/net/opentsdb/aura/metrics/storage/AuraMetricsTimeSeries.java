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

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.metrics.core.data.Memory;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.ByteArrayPool;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.ByteSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public class AuraMetricsTimeSeries implements TimeSeries, CloseablePooledObject {

    protected PooledObject ts_pool;
    protected PooledObject id_byte_array_pool;
  
    protected QueryNode node;
    protected AuraMetricsQueryResult results;
    protected long tsPointer;
    protected long tagPointer;
    protected int tagLength;
    protected int firstSegmentTime;
    protected int segmentCount;

    protected volatile AuraMetricsByteTimeSeriesId id;
    
    /** A cached hash code ID. */
    protected volatile long cached_hash; 
    
    public AuraMetricsTimeSeries() {
    }
    
    public void reset(
        final QueryNode node,
        final AuraMetricsQueryResult results,
        final long tsPointer,
        final long tagPointer,
        final int tagLength,
        final int firstSegmentTime,
        final int segmentCount) {
      this.node = node;
      this.results = results;
      this.tsPointer = tsPointer;
      this.tagPointer = tagPointer;
      this.tagLength = tagLength;
      this.firstSegmentTime = firstSegmentTime;
      this.segmentCount =  segmentCount;
    }

  @Override
    public TimeSeriesId id() {
      if (id == null) {
        synchronized (this) {
          if (id == null) {
            id = new AuraMetricsByteTimeSeriesId();
          }
        }
      }
      return id;
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {

        if (type == NumericArrayType.TYPE) {
            final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
                new AuraMetricsNumericArrayIterator(
                    results.downsampleConfig(), 
                    results.rateConfig(), 
                    node, 
                    results, 
                    tsPointer, 
                    firstSegmentTime,
                    segmentCount);
            return Optional.of(iterator);
        }
        if (type.equals(NumericType.TYPE)) {
            /*if (dp != null) {
              return Optional.of(new LastIterator());
            } else*/ if (results.downsampleConfig() != null) {
                final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
                    new AuraMetricsNumericArrayIterator(
                        results.downsampleConfig(), results.rateConfig(), node, results, tsPointer, firstSegmentTime, segmentCount);
                return Optional.of(iterator);
            }
            return Optional.of(new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount));
        }
        return Optional.empty();
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
        return new ArrayList() {{
          /*if (dp != null) {
            add(new LastIterator());
          } else*/ if (results.downsampleConfig() != null) {

            add(new AuraMetricsNumericArrayIterator(
                results.downsampleConfig(), results.rateConfig(), node, results, tsPointer, firstSegmentTime, segmentCount));
          } else {
            add(new AuraMetricsNumericIterator(node, tsPointer, firstSegmentTime, segmentCount));
          }
        }};
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
        return new ArrayList(1) {{
          if (results.downsampleConfig() != null)
            add(NumericArrayType.TYPE); 
          else 
            add(NumericType.TYPE);
        }};
    }

    @Override
    public void close() {
      node = null;
      results = null;
      id = null;
      cached_hash = 0;
      firstSegmentTime = 0;
      segmentCount = 0;
      if (id_byte_array_pool != null) {
        id_byte_array_pool.release();
        id_byte_array_pool = null;
      }
      release();
    }
    
    public class AuraMetricsByteTimeSeriesId implements TimeSeriesByteId {

      long tags_pointer;
      
      @Override
      public boolean encoded() {
        return true;
      }

      @Override
      public TypeToken<? extends TimeSeriesId> type() {
        return Const.TS_BYTE_ID;
      }

      @Override
      public long buildHashCode() {
        return tags_pointer;
      }

      @Override
      public int compareTo(final TimeSeriesByteId o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public TimeSeriesDataSourceFactory dataStore() {
        return (TimeSeriesDataSourceFactory) ((AbstractQueryNode) node).factory();
      }

      @Override
      public byte[] alias() {
        return null;
      }

      @Override
      public byte[] namespace() {
        // TODO could get the configured instance namespace.
        return null;
      }

      @Override
      public byte[] metric() {
        return results.metric();
      }

      @Override
      public Map<byte[], byte[]> tags() {
        if (id_byte_array_pool != null) {
          return new AuraTagMap(tagLength, (byte[]) id_byte_array_pool.object());
        }
        // We purposely want to read and then toss the buffer here so we hold on
        // to the memory as little as possible.
        // TODO - Use a pooled or pre-allocated buffer after investigating the
        // threading issues.
        final ObjectPool pool = node.pipelineContext().tsdb().getRegistry()
            .getObjectPool(ByteArrayPool.TYPE);
        final byte[] buf;
        if (pool != null) {
          id_byte_array_pool = ((ArrayObjectPool) pool).claim(tagLength);
          buf = (byte[]) id_byte_array_pool.object();
        } else {
          buf = new byte[tagLength];
        }
        // TODO - pool
        Memory.read(tagPointer, buf, tagLength);
        final AuraTagMap map = new AuraTagMap(tagLength, buf);
        return map;
      }

      @Override
      public List<byte[]> aggregatedTags() {
        // TODO implement
        return null;
      }

      @Override
      public List<byte[]> disjointTags() {
        // TODO implement
        return null;
      }

      @Override
      public ByteSet uniqueIds() {
        return null;
      }

      @Override
      public boolean skipMetric() {
        return false;
      }

      @Override
      public Deferred<TimeSeriesStringId> decode(boolean cache, Span span) {
        final Map<byte[], byte[]> byte_tags = tags();
        final Map<String, String> tags = Maps.newHashMapWithExpectedSize(byte_tags.size());
        for (final Entry<byte[], byte[]> entry : byte_tags.entrySet()) {
          tags.put(new String(entry.getKey(), Const.UTF8_CHARSET), 
              new String(entry.getValue(), Const.UTF8_CHARSET));
        }
        // TODO - agg/disjoint tags.
        return Deferred.fromResult(BaseTimeSeriesStringId.newBuilder()
            .setMetric(new String(results.metric(), Const.UTF8_CHARSET))
            .setTags(tags)
            .build());
      }
      
    }

    @Override
    public Object object() {
      return this;
    }

    @Override
    public void release() {
      if (ts_pool != null) {
        ts_pool.release();
      }
    }

    @Override
    public void setPooledObject(PooledObject pooled_object) {
      this.ts_pool = pooled_object;
      
    }
    
}
