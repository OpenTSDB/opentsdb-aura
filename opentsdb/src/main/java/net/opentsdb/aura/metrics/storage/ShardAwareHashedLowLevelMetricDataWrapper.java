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

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.metrics.core.ShardAware;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.LowLevelMetricData.HashedLowLevelMetricData;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pools.BaseObjectPoolAllocator;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;

import java.io.IOException;

public class ShardAwareHashedLowLevelMetricDataWrapper implements
        ShardAware,
        HashedLowLevelMetricData,
        CloseablePooledObject,
        Runnable {

  public HashedLowLevelMetricData data;
  public TimeSeriesShardIF shard;
  private PooledObject pooledObject;

  @Override
  public void setShard(TimeSeriesShardIF shard) {
    this.shard = shard;
  }

  @Override
  public long metricHash() {
    return data.metricHash();
  }

  @Override
  public StringFormat metricFormat() {
    return data.metricFormat();
  }

  @Override
  public int metricStart() {
    return data.metricStart();
  }

  @Override
  public int metricLength() {
    return data.metricLength();
  }

  @Override
  public byte[] metricBuffer() {
    return data.metricBuffer();
  }

  @Override
  public ValueFormat valueFormat() {
    return data.valueFormat();
  }

  @Override
  public long longValue() {
    return data.longValue();
  }

  @Override
  public float floatValue() {
    return data.floatValue();
  }

  @Override
  public double doubleValue() {
    return data.doubleValue();
  }

  @Override
  public long timeSeriesHash() {
    return data.timeSeriesHash();
  }

  @Override
  public long tagsSetHash() {
    return data.tagsSetHash();
  }

  @Override
  public long tagPairHash() {
    return data.tagPairHash();
  }

  @Override
  public long tagKeyHash() {
    return data.tagKeyHash();
  }

  @Override
  public long tagValueHash() {
    return data.tagValueHash();
  }

  @Override
  public boolean advance() {
    return data.advance();
  }

  @Override
  public boolean hasParsingError() {
    return data.hasParsingError();
  }

  @Override
  public String parsingError() {
    return data.parsingError();
  }

  @Override
  public TimeStamp timestamp() {
    return data.timestamp();
  }

  @Override
  public byte[] tagsBuffer() {
    return data.tagsBuffer();
  }

  @Override
  public int tagBufferStart() {
    return data.tagBufferStart();
  }

  @Override
  public int tagBufferLength() {
    return data.tagBufferLength();
  }

  @Override
  public StringFormat tagsFormat() {
    return data.tagsFormat();
  }

  @Override
  public byte tagDelimiter() {
    return data.tagDelimiter();
  }

  @Override
  public int tagSetCount() {
    return data.tagSetCount();
  }

  @Override
  public boolean advanceTagPair() {
    return data.advanceTagPair();
  }

  @Override
  public int tagKeyStart() {
    return data.tagKeyStart();
  }

  @Override
  public int tagKeyLength() {
    return data.tagKeyLength();
  }

  @Override
  public int tagValueStart() {
    return data.tagValueStart();
  }

  @Override
  public int tagValueLength() {
    return data.tagKeyLength();
  }

  @Override
  public boolean commonTags() {
    return data.commonTags();
  }

  @Override
  public boolean commonTimestamp() {
    return data.commonTimestamp();
  }

  @Override
  public void close() throws IOException {
    if (data != null) {
      data.close();
    }
    release();
  }

  @Override
  public void run() {
    shard.addEvent(this);
  }

  @Override
  public void setPooledObject(final PooledObject pooledObject) {
    this.pooledObject = pooledObject;
  }

  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (pooledObject != null) {
      pooledObject.release();
    }
  }

  static class ShardAwareWrapperAllocator extends BaseObjectPoolAllocator {
    public static final String TYPE = "ShardAwareWrapperAllocator";
    private static final TypeToken<?> TYPE_TOKEN =
            TypeToken.of(ShardAwareHashedLowLevelMetricDataWrapper.class);

    @Override
    public Object allocate() {
      return new ShardAwareHashedLowLevelMetricDataWrapper();
    }

    @Override
    public TypeToken<?> dataType() {
      return TYPE_TOKEN;
    }

    @Override
    public String type() {
      return TYPE;
    }

    @Override
    public Deferred<Object> initialize(TSDB tsdb, String s) {
      return Deferred.fromResult(null);
    }
  }
}
