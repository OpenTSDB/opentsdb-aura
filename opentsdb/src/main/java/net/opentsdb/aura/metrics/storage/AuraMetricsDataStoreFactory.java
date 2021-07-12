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

import java.io.IOException;
import java.util.List;
import java.util.Random;

import net.opentsdb.aura.metrics.core.ShardAware;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.aura.metrics.core.TimeSeriesStorageIf;
import net.opentsdb.aura.metrics.pools.LowLevelMetricShardContainerPool;
import net.opentsdb.aura.metrics.storage.ShardAwareHashedLowLevelMetricDataWrapper.ShardAwareWrapperAllocator;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.storage.TimeSeriesDataConsumer;
import net.opentsdb.storage.TimeSeriesDataConsumer.WriteCallback;
import net.opentsdb.storage.TimeSeriesDataConsumerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.LowLevelMetricData.HashedLowLevelMetricData;
import net.opentsdb.pools.BlockingQueueObjectPool;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.ObjectPoolConfig;
import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.WriteStatus;

/**
 * TODO - properly break this up. For now it's just one big ole mess.
 */
public class AuraMetricsDataStoreFactory extends BaseTSDBPlugin
        implements TimeSeriesDataConsumer,
        TimeSeriesDataConsumerFactory {
  private static Logger logger = LoggerFactory.getLogger(AuraMetricsDataStoreFactory.class);
  public static final String TYPE = AuraMetricsDataStoreFactory.class.toString();

  public final ThreadLocal<LowLevelMetricShardContainer[]> shardContainers;
  private final TimeSeriesStorageIf timeSeriesStorage;

  public LowLevelMetricShardContainerPool allocator;
  public ThreadLocal<ObjectPool> shardContainerPools;
  public ObjectPool shardAwareWrapperPool;

  private boolean process_deferreds;
  private Random rnd = new Random(System.currentTimeMillis());

  public AuraMetricsDataStoreFactory(final TimeSeriesStorageIf timeSeriesStorage) {
    this.timeSeriesStorage = timeSeriesStorage;
    shardContainers = ThreadLocal.withInitial(() ->
            new LowLevelMetricShardContainer[timeSeriesStorage.numShards()]);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;

    allocator = new LowLevelMetricShardContainerPool();
    shardContainerPools = 
        new ThreadLocal<ObjectPool>() {
          @Override
          protected ObjectPool initialValue() {
            ObjectPoolConfig poolConfig = DefaultObjectPoolConfig.newBuilder()
                .setInitialCount(64)
                .setMaxCount(64)
                .setAllocator(allocator)
                .setId("LowLevelMetricShardContainerPool")
                .build();
            return new BlockingQueueObjectPool(tsdb, poolConfig);
          }
        };

    ObjectPoolConfig poolConfig = DefaultObjectPoolConfig.newBuilder()
            // TODO config
            .setInitialCount(4096)
            .setMaxCount(4096)
            .setAllocator(new ShardAwareWrapperAllocator())
            .setId("ShardAwareWrapperAllocator")
            .build();
    shardAwareWrapperPool = new BlockingQueueObjectPool(tsdb, poolConfig);
    return Deferred.fromResult(null);
  }
  
  @Override
  public void write(final AuthState state,
                    final TimeSeriesDatum datum,
                    final WriteCallback callback) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void write(final AuthState state,
                    final TimeSeriesSharedTagsAndTimeData data,
                    final WriteCallback callback) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void write(final AuthState state,
                    final LowLevelTimeSeriesData data,
                    final WriteCallback callback) {
    if (!(data instanceof HashedLowLevelMetricData)) {
      logger.warn("Not a hashed low level metric container: " + data.getClass());
      // bad!?!?!?!
    } else if (data.commonTags()) {
      // sweet, then we wrap or pass it down
      if (data instanceof ShardAware) {
        timeSeriesStorage.addEvent((HashedLowLevelMetricData) data);
      } else {
        final ShardAwareHashedLowLevelMetricDataWrapper wrapper =
                (ShardAwareHashedLowLevelMetricDataWrapper) shardAwareWrapperPool.claim();
        wrapper.data = (HashedLowLevelMetricData) data;
        timeSeriesStorage.addEvent(wrapper);
      }

      // TODO - process deferreds.
    } else {
      // here's where things get "fun". We need to split the payload and find
      // the proper shard.
      ObjectPool pool = shardContainerPools.get();
      LowLevelMetricShardContainer[] shardContainers = new LowLevelMetricShardContainer[timeSeriesStorage.numShards()];//SHARD_CONTAINERS.get();
      LowLevelMetricShardContainer container = (LowLevelMetricShardContainer) pool.claim().object();
      HashedLowLevelMetricData hashed = (HashedLowLevelMetricData) data;

      // This is some code to play around with a direct shim. It won't work for our
      // use case.
//      if (true) {
//        TimeSeriesShard shrd = TimeSeriesStorage.getShard(0);
//        TsdbDataShim shim = new TsdbDataShim(hashed, shrd);
//        try {
//          shrd.put((Runnable) shim);
//        } catch (InterruptedException e) {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//        }
//        return Deferred.fromResult(null);
//      }
      int currentTimeHour = (int) System.currentTimeMillis() / 1000;
      currentTimeHour = currentTimeHour - (currentTimeHour % 3600);
      while (hashed.advance()) {
        final int segmentHour = timeSeriesStorage.getSegmentHour((int) data.timestamp().epoch());
        if ((currentTimeHour - segmentHour) / 3600 > timeSeriesStorage.retentionInHours()) { // older than retention period. drop it.
          //delayedTimeSeriesCounter.inc();
          if (process_deferreds) {
            int read = 0;
            while (data.advance()) {
              read++;
            }
            // TODO - callback
            return;
          }
          
          try {
            data.close();
          } catch (IOException e) {
            logger.error("Unexpected exception closing original data source", e);
          }
          continue;
        }
    
        if (segmentHour > currentTimeHour) { // too early, drop it.
          //earlyTimeSeriesCounter.inc();
          if (process_deferreds) {
            int read = 0;
            while (data.advance()) {
              read++;
            }
            // TODO - callback
          }
          
          try {
            data.close();
          } catch (IOException e) {
            logger.error("Unexpected exception closing original data source", e);
          }
          continue;
        }
        
        final int shardId = timeSeriesStorage.getShardId(hashed.tagsSetHash());
        if (shardContainers[shardId] == null) {
          shardContainers[shardId] = (LowLevelMetricShardContainer) pool.claim().object();
        }
        
        shardContainers[shardId].append(hashed);
      }
      
      for (int i = 0; i < shardContainers.length; i++) {
        if (shardContainers[i] == null) {
          continue;
        }
        
        try {
          shardContainers[i].shard = timeSeriesStorage.getShard(i);
          shardContainers[i].finishWrite(); // NOTE: Super important.
          shardContainers[i].shard.submit((Runnable) shardContainers[i]);
        } catch (InterruptedException e) {
          logger.error("Interrupted while adding data to shard queue", e);
        }
        shardContainers[i] = null;
      }
      
      try {
        data.close();
      } catch (IOException e) {
        logger.error("Unexpected exception closing original data source", e);
      }
      
    }
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public TimeSeriesDataConsumer consumer() {
    return this;
  }

}