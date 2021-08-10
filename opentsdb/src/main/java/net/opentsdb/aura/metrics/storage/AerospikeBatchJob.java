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

import net.opentsdb.aura.metrics.AerospikeRecordMap;
import net.opentsdb.aura.metrics.BatchRecordIterator;
import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.gorilla.GorillaRawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.OnHeapGorillaRawSegment;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 *
 * INVARIATES:
 * - endHashId MUST be &gt; 0 and &gt; startHashId.
 * - endGroupId &gt;= startGroupId
 */
public class AerospikeBatchJob implements Runnable, CloseablePooledObject {
  private static final Logger LOGGER = LoggerFactory.getLogger(AerospikeBatchJob.class);

  private static final ThreadLocal<ASBatchKeys> BATCH_KEYS =
          ThreadLocal.withInitial(() -> new ASBatchKeys());

  protected ASBatchKeys batchKeys;
  protected AerospikeRecordMap map;
  protected AerospikeBatchQueryNode queryNode;
  protected MetaTimeSeriesQueryResult metaResult;
  protected CountDownLatch latch;
  protected TLongObjectMap<AggregatorFinishedTS> groups;
  AerospikeBatchGroupAggregator grouper;

  protected int startGroupId;
  protected int endGroupId;
  protected int startHashId;
  protected int endHashId;
  protected int nextRecord;

  protected PooledObject pooledObject;
  protected OnHeapGorillaRawSegment segment;
  protected GorillaRawTimeSeriesEncoder encoder;

  // some stats;
  protected volatile long totalAStime;
  protected volatile long totalDecodeTime;
  protected volatile int totalRuns;
  protected volatile int keyHits;

  public AerospikeBatchJob() {
    map = new AerospikeRecordMap();
    groups = new TLongObjectHashMap<AggregatorFinishedTS>();
    grouper = new AerospikeBatchGroupAggregator();
    segment = new OnHeapGorillaRawSegment();
    encoder = new GorillaRawTimeSeriesEncoder(false, null, segment, null);
  }

  public void reset(final AerospikeBatchQueryNode queryNode,
                    final MetaTimeSeriesQueryResult metaResult,
                    final CountDownLatch latch) {
    this.queryNode = queryNode;
    this.metaResult = metaResult;
    this.latch = latch;
    close();
  }

  public void close() {
    grouper.resetInit();
    groups.clear();
    startGroupId = 0;
    endGroupId = 0;
    startHashId = 0;
    endHashId = 0;
    nextRecord = 0;

    totalAStime = 0;
    totalDecodeTime = 0;
    totalRuns = 0;
    keyHits = 0;

    release();
  }

  @Override
  public void run() {
    if (queryNode.pipelineContext().queryContext().isClosed()) {
      complete(null);
      return;
    }
    ++totalRuns;
    try {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("STARTING...Hash {} SG {} EG {} SH {} EH {}",
                System.identityHashCode(this), startGroupId, endGroupId, startHashId, endHashId);
      }
      batchKeys = BATCH_KEYS.get();
      batchKeys.keyIdx = 0;

      while (true) {
        MetaTimeSeriesQueryResult.GroupResult gr = metaResult.getGroup(startGroupId);
        int numHashes = gr.numHashes();
        if (nextRecord > 0) {
          // continuation of a series
          if (!setupAndOrSendBatch(gr.getHash(startHashId),gr.getBitMap(startHashId), gr.id())) {
            break;
          } else {
            if (!increment()) {
              break;
            }
            continue;
          }
        } else if (startHashId > 0) {
          if (endGroupId == startGroupId && endHashId > 0) {
            // range within this group and that'd be it.
            for (; startHashId < endHashId; startHashId++) {
              if (!setupAndOrSendBatch(gr.getHash(startHashId),gr.getBitMap(startHashId), gr.id())) {
                return;
              }
            }
            break;
          } else {
            // in this case, we ignore the end hash. If set, it's set
            // for the final GB index we're reading.
            for (; startHashId < numHashes; startHashId++) {
              if (!setupAndOrSendBatch(gr.getHash(startHashId), gr.getBitMap(startHashId), gr.id())) {
                return;
              }
            }

            // advance
            startGroupId++;
            startHashId = 0;
          }
        } else {
          if (endGroupId == startGroupId) {
            // tail end
            for (; startHashId < endHashId; startHashId++) {
              if (!setupAndOrSendBatch(gr.getHash(startHashId), gr.getBitMap(startHashId), gr.id())) {
                return;
              }
            }
            break;
          } else {
            // all in this group
            for (; startHashId < numHashes; startHashId++) {
              if (!setupAndOrSendBatch(gr.getHash(startHashId), gr.getBitMap(startHashId), gr.id())) {
                return;
              }
            }

            // advance
            startGroupId++;
            startHashId = 0;
          }
        }
      }

      // remainder
      if (batchKeys.keyIdx > 0) {
        sendBatch();
      }

      complete(null);
    } catch (Throwable t) {
      complete(t);
    }
  }

  boolean setupAndOrSendBatch(final long hash, final RoaringBitmap bitmap, final long groupId) {
    batchKeys.grow(queryNode.batchLimit());
    int ts;
    if (nextRecord > 0) {
      ts = nextRecord;
      nextRecord = 0;
    } else {
      ts = queryNode.segmentsStart();
    }

    while (ts < queryNode.segmentsEnd()) {

      if(bitmap != null && !bitmap.contains(ts)) {
        //No need to generate Aerospike hash for this epoch.
        ts += queryNode.secondsInRecord();
        continue;
      }

      // not bothering with getters/setters here.
      int keyIdx = batchKeys.keyIdx;
      ByteArrays.putLong(hash, batchKeys.keys[keyIdx], 0);
      ByteArrays.putInt(ts, batchKeys.keys[keyIdx], 8);
      batchKeys.groupIds[keyIdx] = groupId;
      batchKeys.groupIdx[keyIdx] = startGroupId;
      batchKeys.hashes[keyIdx] = hash;
      ++batchKeys.keyIdx;
      ts += queryNode.secondsInRecord();

      if (batchKeys.keyIdx >= queryNode.batchLimit()) {
        // gotta send so checkpoint here.
        sendBatch();
        if (ts >= queryNode.segmentsEnd()) {
          // we need to increment to the next series or group
          if (increment()) {
            queryNode.submit(this);
          }

          return false;
        }

        // don't increment anything, just set the next timestamp to resume at.
        nextRecord = ts;
        queryNode.submit(this);
        return false;
      }
    }
    return true;
  }

  boolean increment() {
    if (startGroupId == endGroupId) {
      // last group. So we see if we're a range or the tail
      if (startHashId + 1 >= endHashId) {
        // done! No reschedule
        complete(null);
        return false;
      } else {
        ++startHashId;
        return true;
      }
    } else {
      int numHashes = metaResult.getGroup(startGroupId).numHashes();
      if (startHashId + 1 >= numHashes) {
        startHashId = 0;
        startGroupId++;
        return true;
      } else {
        startHashId++;
        return true;
      }
    }
  }

  void sendBatch() {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("SENDING BATCH:  keys: {}  batchLimit {}", batchKeys.keyIdx, queryNode.batchLimit());
    }

    long st = System.nanoTime();
    BatchRecordIterator bri = queryNode.client().batchRead(batchKeys.keys, batchKeys.keyIdx);
    long asTime = System.nanoTime() - st;
    totalAStime += asTime;
    st = System.nanoTime();
    processBatch(bri);
    long procTime = System.nanoTime() - st;
    totalDecodeTime += procTime;

    if (LOGGER.isTraceEnabled()) {
      LOGGER.info("Finished with a batch. AS Time {}ms Proc Time {}ms. Keys {}",
              ((double) asTime / (double) 1_000_000),
              ((double) procTime / (double) 1_000_000),
              batchKeys.keyIdx);
    }
  }

  void complete(final Throwable t) {
    if (t != null) {
      LOGGER.error("Failed job", t);
      queryNode.onError(t);
    }

    NumericArrayAggregator agg = grouper.flush();
    if (agg != null && agg.end() > agg.offset()) {
      // grab the last key.
      MetaTimeSeriesQueryResult.GroupResult gr = metaResult.getGroup(endGroupId);
      groups.put(grouper.gid(), new AggregatorFinishedTS(queryNode, gr, agg));
    }

    if (latch != null) {
      latch.countDown();
    }
  }

  void processBatch(BatchRecordIterator bri) {
    // SO... for downsampling and rate we need data in order. But batches are
    // emphatically NOT in order. So here is one super inefficient and ugly way
    // to re-order our keys.
    if (bri.getResultCode() == 2) {
      // no keys found
      return;
    }
    if (bri.getResultCode() != 0) {
      // TODO - error!
    }

    bri.sort();
    keyHits += bri.entries();

    for (int i = 0; i < bri.entries(); i++) {
      int keyIdx = bri.keyIndices()[i];
      long hash = batchKeys.hashes[keyIdx];
      byte[] key = batchKeys.keys[keyIdx];
      long groupId = batchKeys.groupIds[keyIdx];

      int lastSeggy = 0;
      // check flush
      if (!grouper.isInit() || grouper.gid() != groupId) {
        if (grouper.isInit()) {
          NumericArrayAggregator agg = grouper.flush();
          if (agg != null && agg.end() > agg.offset()) {
            MetaTimeSeriesQueryResult.GroupResult gr = metaResult.getGroup(grouper.gidx());
            groups.put(grouper.gid(), new AggregatorFinishedTS(queryNode, gr, agg));
          }
        }
        grouper.reset(groupId, batchKeys.groupIdx[keyIdx], queryNode.queryResult(), metaResult);
      }

      int recordTimestamp = ByteArrays.getInt(key, 8);
      map.reset(bri.buffer(), bri.particleIndex(i), bri.particleLength(i));
      for (int x = 0; x < map.size(); x++) {
        int segmentTimestamp = recordTimestamp + (map.keys()[x] * queryNode.getSecondsInSegment());
        // TODO - cache results. For now we're just putting on heap then passing into
        // the group by.
        map.encoder(segment, segmentTimestamp, x);
        grouper.addSegment(encoder, hash);
      }
    }

  }

  @Override
  public void setPooledObject(PooledObject pooledObject) {
    if (this.pooledObject != null) {
      this.pooledObject.release();
    }
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
}