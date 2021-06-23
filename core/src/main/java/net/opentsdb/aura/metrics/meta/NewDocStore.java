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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.opentsdb.aura.metrics.core.ShardAware;
import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.aura.metrics.core.Util;
import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.data.HashTable;
import net.opentsdb.aura.metrics.core.data.InSufficientBufferLengthException;
import net.opentsdb.aura.metrics.core.data.Memory;
import net.opentsdb.aura.metrics.meta.Filter.Operator;
import net.opentsdb.aura.metrics.meta.Filter.StringType;
import net.opentsdb.aura.metrics.meta.Filter.Type;
import net.opentsdb.aura.metrics.meta.MetaQueryBuilder.MetaQueryType;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.hashing.HashFunction;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

public class NewDocStore implements MetaDataStore, ShardAware {

  private static Logger logger = LoggerFactory.getLogger(NewDocStore.class);

  private static final int DEFAULT_INITIAL_CAPACITY = 1024;
  private static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;

  /**
   * Magic string to store metrics doc bitmaps.
   */
  public static final String METRICS = "__AURAMETRIC";

  private TimeSeriesShardIF shard; // the shard we belong to. Needed for Meta queries.
  private final Map<String, Map<String, RoaringBitmap>> indexMap;
  private final RoaringBitmap resets = new RoaringBitmap();
  private final List<long[]> tables;
  private final int initialCapacity;
  private final int maxCapacity;
  private volatile int currentIndex;
  private int size;

  private final DocStoreQuery queryRunner;
  private final SharedMetaResult shardMetaResults;
  private final HashFunction hashFunction;

  private final int[] tableIndices; // = new int[DOCID_BATCH_SIZE]; // L2 cache aligned.
  private final int[] docIndices; // = new int[DOCID_BATCH_SIZE]; // L2 cache aligned.
  private final int[] indices; // = new int[DOCID_BATCH_SIZE]; // L2 cache aligned.
  private final long[] histo = new long[4];
  private boolean metaQueryEnabled;

  public static final NewDocStore newInstance(final TimeSeriesShardIF shard, final boolean metaQueryEnabled) {
    return new NewDocStore(DEFAULT_INITIAL_CAPACITY, TimeSeriesShardIF.DOCID_BATCH_SIZE, shard, null, null, metaQueryEnabled);
  }

  public NewDocStore(final TimeSeriesShardIF shard) {
    this(DEFAULT_INITIAL_CAPACITY, TimeSeriesShardIF.DOCID_BATCH_SIZE, shard, null, null, false);
  }

  public NewDocStore(
      final int initialCapacity,
      final int purgeBatchSize,
      final TimeSeriesShardIF shard,
      final HashFunction hashFunction,
      final SharedMetaResult shardMetaResults,
      final boolean metaQueryEnabled) {
    this(purgeBatchSize, shard, hashFunction, initialCapacity, DEFAULT_MAX_CAPACITY, shardMetaResults, metaQueryEnabled);
  }

  public NewDocStore(
      final int purgeBatchSize,
      final TimeSeriesShardIF shard,
      final HashFunction hashFunction,
      final int initialCapacity,
      final int maxCapacity,
      final SharedMetaResult shardMetaResults,
      final boolean metaQueryEnabled) {

    if (maxCapacity < initialCapacity) {
      throw new IllegalArgumentException(
          "max capacity should be greater than the initial capacity");
    }
    this.tableIndices = new int[purgeBatchSize];
    this.docIndices = new int[purgeBatchSize];
    this.indices = new int[purgeBatchSize];
    this.hashFunction = hashFunction;
    this.shard = shard;
    this.initialCapacity = initialCapacity;
    this.maxCapacity = maxCapacity;
    this.tables = new ArrayList<>();
    this.tables.add(new long[initialCapacity]);
    this.indexMap = new HashMap<>();
    this.queryRunner = new DocStoreQuery();
    this.shardMetaResults = shardMetaResults;
    this.metaQueryEnabled = metaQueryEnabled;
  }

  @Override
  public void setShard(TimeSeriesShardIF shard) {
    this.shard = shard;
  }

  public void add(final String[] tagKeys, final String[] tagValues, long key) {

    final int index;
    if (resets.isEmpty()) {
      index = currentIndex++;
    } else {
      index = resets.first();
      resets.remove(index);
    }

    if (index == maxCapacity) {
      throw new IllegalStateException("Maximum meta store capacity reached: " + maxCapacity);
    }

    final int tableIndex = index / initialCapacity;
    final int docIndex = index % initialCapacity;

    long[] docs;
    if (tableIndex + 1 > tables.size()) { // grow table.
      docs = new long[initialCapacity];
      tables.add(docs);
    } else {
      docs = tables.get(tableIndex);
    }

    docs[docIndex] = key;

    for (int i = 0; i < tagKeys.length; i++) {
      String tagKey = tagKeys[i];
      String tagValue = tagValues[i];

      Map<String, RoaringBitmap> valueMap =
          this.indexMap.computeIfAbsent(tagKey, (v) -> new HashMap<>());

      RoaringBitmap rr = valueMap.get(tagValue);
      if (rr == null) {
        valueMap.put(tagValue, RoaringBitmap.bitmapOf(index));
      } else {
        rr.add(index);
      }
    }

    size++;
  }

//  // TODO: Change from BaseMetricEvent to MetricEvent?
//  public void add(BaseMetricsEvent event, int metricIndex) {
//    final int index;
//    if (resets.isEmpty()) {
//      index = currentIndex++;
//    } else {
//      index = resets.first();
//      resets.remove(index);
//    }
//
//    if (index == maxCapacity) {
//      throw new IllegalStateException("Maximum meta store capacity reached: " + maxCapacity);
//    }
//
//    final int tableIndex = index / initialCapacity;
//    final int docIndex = index % initialCapacity;
//
//    long[] docs;
//    if (tableIndex + 1 > tables.size()) { // grow table.
//      docs = new long[initialCapacity];
//      tables.add(docs);
//    } else {
//      docs = tables.get(tableIndex);
//    }
//
//    docs[docIndex] = event.getTagHash();
//
//    int idx = 0;
//    for (int i = 0; i < event.getTagCount(); i++) {
//      // TODO - see if we can store copies of the byte arrays in the maps.
//      int end = event.findTagEnd(idx);
//
//      String tagKey = new String(event.getTags(), idx, end - idx, UTF8);
//      idx = end + 1;
//      end = event.findTagEnd(idx);
//      String tagValue = new String(event.getTags(), idx, end - idx, UTF8);
//      idx = end + 1;
//      Map<String, RoaringBitmap> valueMap =
//          this.indexMap.computeIfAbsent(tagKey, (v) -> new HashMap<>());
//
//      RoaringBitmap rr = valueMap.get(tagValue);
//      if (rr == null) {
//        valueMap.put(tagValue, RoaringBitmap.bitmapOf(index));
//      } else {
//        rr.add(index);
//      }
//    }
//
//    Map<String, RoaringBitmap> valueMap =
//        this.indexMap.computeIfAbsent(METRICS, (v) -> new HashMap<>());
//    String m = new String(event.getMetric(metricIndex), UTF8);
//    RoaringBitmap rr = valueMap.get(m);
//    if (rr == null) {
//      valueMap.put(m, RoaringBitmap.bitmapOf(index));
//    } else {
//      rr.add(index);
//    }
//
//    size += 2;
//  }

  @Override
  public void add(LowLevelMetricData.HashedLowLevelMetricData container) {
    final int index;
    if (resets.isEmpty()) {
      index = currentIndex++;
    } else {
      index = resets.first();
      resets.remove(index);
    }

    if (index == maxCapacity) {
      throw new IllegalStateException("Maximum meta store capacity reached: " + maxCapacity);
    }

    final int tableIndex = index / initialCapacity;
    final int docIndex = index % initialCapacity;

    long[] docs;
    if (tableIndex + 1 > tables.size()) { // grow table.
      docs = new long[initialCapacity];
      tables.add(docs);
    } else {
      docs = tables.get(tableIndex);
    }

    docs[docIndex] = container.tagsSetHash();
    while (container.advanceTagPair()) {
      String tagKey =
          new String(
              container.tagsBuffer(), container.tagKeyStart(), container.tagKeyLength(), StandardCharsets.UTF_8);
      String tagValue =
          new String(
              container.tagsBuffer(),
              container.tagValueStart(),
              container.tagValueLength(),
              StandardCharsets.UTF_8); // tagValues[i]
      Map<String, RoaringBitmap> valueMap =
          this.indexMap.computeIfAbsent(tagKey, (v) -> new HashMap<>());

      RoaringBitmap rr = valueMap.get(tagValue);
      if (rr == null) {
        valueMap.put(tagValue, RoaringBitmap.bitmapOf(index));
      } else {
        rr.add(index);
      }
    }

    if (metaQueryEnabled && container instanceof LowLevelMetricData) {
      Map<String, RoaringBitmap> valueMap =
          this.indexMap.computeIfAbsent(METRICS, (v) -> new HashMap<>());
      String m =
          new String(
              container.metricBuffer(), container.metricStart(), container.metricLength(), StandardCharsets.UTF_8);
      RoaringBitmap rr = valueMap.get(m);
      if (rr == null) {
        valueMap.put(m, RoaringBitmap.bitmapOf(index));
      } else {
        rr.add(index);
      }
    }
    size++;
  }

  /**
   * Adds the metric to the doc store and adds/increments the bitmap with the tag set indices that
   * are associated with that metric.
   *
   * @param container The non-null container advanced to the current metric and tag set to store.
   */
  @Override
  public void addMetric(LowLevelMetricData.HashedLowLevelMetricData container) {
    // TODO - now we need to find the index of the tag set. It's ugly. We have
    // to hunt for the lowest cardinality map for the tag values, then iterate
    // until we match the tag set hash. UGG!!!!!!!
    RoaringBitmap merged = null;
    while (container.advanceTagPair()) {
      String tagKey =
          new String(
              container.tagsBuffer(), container.tagKeyStart(), container.tagKeyLength(), StandardCharsets.UTF_8);
      String tagValue =
          new String(
              container.tagsBuffer(),
              container.tagValueStart(),
              container.tagValueLength(),
                  StandardCharsets.UTF_8);
      Map<String, RoaringBitmap> valueMap = indexMap.get(tagKey);
      if (valueMap == null) {
        logger.error(
            Thread.currentThread().getName()
                + " No entry found for tag key: "
                + tagKey
                + " FROM ["
                + new String(
                container.metricBuffer(),
                container.metricStart(),
                container.metricLength(),
                    StandardCharsets.UTF_8)
                + "] "
                + new String(
                container.tagsBuffer(),
                container.tagBufferStart(),
                container.tagBufferLength(),
                    StandardCharsets.UTF_8)
                + " MHash "
                + container.metricHash()
                + " SetHash: "
                + container.tagsSetHash()
                + " SH: "
                + container.timeSeriesHash());
        return;
      }

      RoaringBitmap rr = valueMap.get(tagValue);
      if (rr == null) {
        logger.error(
            Thread.currentThread().getName()
                + " No entry found for pair "
                + tagKey
                + " = "
                + tagValue
                + " FROM ["
                + new String(
                container.metricBuffer(),
                container.metricStart(),
                container.metricLength(),
                    StandardCharsets.UTF_8)
                + "] "
                + new String(
                container.tagsBuffer(),
                container.tagBufferStart(),
                container.tagBufferLength(),
                    StandardCharsets.UTF_8)
                + " MHash "
                + container.metricHash()
                + " SetHash: "
                + container.tagsSetHash()
                + " SH: "
                + container.timeSeriesHash());
        return;
      } else {
        if (merged == null) {
          merged = new RoaringBitmap();
          merged.or(rr);
        } else {
          merged.and(rr);
        }
      }
    }

    if (merged == null) {
      logger.error(
          Thread.currentThread().getName()
              + " No entry found for tag set "
              + container.tagsSetHash()
              + "  FROM ["
              + new String(
              container.metricBuffer(), container.metricStart(), container.metricLength(), StandardCharsets.UTF_8)
              + "] "
              + new String(
              container.tagsBuffer(),
              container.tagBufferStart(),
              container.tagBufferLength(),
                  StandardCharsets.UTF_8)
              + " MHash "
              + container.metricHash()
              + " SetHash: "
              + container.tagsSetHash()
              + " SH: "
              + container.timeSeriesHash());
      return;
    }

    long hash = container.tagsSetHash();
    final PeekableIntIterator it = merged.getIntIterator();
    boolean found = false;
    int index = 0;
    while (it.hasNext()) {
      index = it.next();
      final int tableIndex = index / initialCapacity;
      final int docIndex = index % initialCapacity;
      if (tables.get(tableIndex)[docIndex] == hash) {
        found = true;
        // WOOT!
        break;
      }
    }

    if (!found) {
      logger.error(
          Thread.currentThread().getName()
              + " This shouldn't happen. Couldn't find an "
              + "entry for hash: "
              + hash
              + " FROM ["
              + new String(
              container.metricBuffer(), container.metricStart(), container.metricLength(), StandardCharsets.UTF_8)
              + "] "
              + new String(
              container.tagsBuffer(),
              container.tagBufferStart(),
              container.tagBufferLength(),
                  StandardCharsets.UTF_8)
              + " MHash "
              + container.metricHash()
              + " SetHash: "
              + container.tagsSetHash()
              + " SH: "
              + container.timeSeriesHash()
              + " RR: "
              + merged);
      return;
    }

    Map<String, RoaringBitmap> valueMap =
        this.indexMap.computeIfAbsent(METRICS, (v) -> new HashMap<>());
    String m =
        new String(
            container.metricBuffer(), container.metricStart(), container.metricLength(), StandardCharsets.UTF_8);
    RoaringBitmap rr = valueMap.get(m);
    if (rr == null) {
      valueMap.put(m, RoaringBitmap.bitmapOf(index));
    } else {
      rr.add(index);
    }
  }

  @Override
  public SharedMetaResult queryMeta(MetaQuery query) {
    shardMetaResults.reset(query);
    queryRunner.run();
    return shardMetaResults;
  }

  @Override
  public void remove(final long[] docIdBatch, final int batchSize, byte[] byteBuffer) throws InSufficientBufferLengthException {

    histo[0] = 0; // sum
    histo[1] = 0; // count
    histo[2] = Long.MAX_VALUE; // min
    histo[3] = Long.MIN_VALUE; // max

    long start = System.nanoTime();

    Arrays.sort(docIdBatch, 0, batchSize);
    int n = 0;

    for (int i = 0; i < tables.size(); i++) {
      long[] table = tables.get(i);
      for (int j = 0; j < table.length; j++) {
        long docId = table[j];
        if (docId == 0) {
          continue;
        }
        if (Arrays.binarySearch(docIdBatch, 0, batchSize, docId) >= 0) { // doc found
          tableIndices[n] = i;
          docIndices[n] = j;
          n++;
        }
        if (n == batchSize) {
          break;
        }
      }
    }

    long sortLatency = System.nanoTime() - start;
    logger.info("DocIdBatch sort and search shardId: {} batchSize: {} match: {} latency: {} ns docStoreSize: {}", shard.getId(), batchSize, n, sortLatency, size);

    AtomicLong tagPurgeLatency = new AtomicLong();
    for (int i = 0; i < n; i++) {
      int tableIndex = tableIndices[i];
      int docIndex = docIndices[i];
      int index = tableIndex * initialCapacity + docIndex;
      indices[i] = index;
      long docId = tables.get(tableIndex)[docIndex];
      int tagSetLength = shard.getAndRemoveTagset(docId, byteBuffer);

      Util.parseTags(byteBuffer, 0, tagSetLength, (tagKey, tagValue) -> {
        long s = System.nanoTime();
        Map<String, RoaringBitmap> valueMap = indexMap.get(tagKey);
        RoaringBitmap bitmap = valueMap.get(tagValue);
        bitmap.remove(index);
        if (bitmap.isEmpty()) {
          valueMap.remove(tagValue);
        }
        if (valueMap.isEmpty()) {
          indexMap.remove(tagKey);
        }
        tagPurgeLatency.getAndAdd(System.nanoTime() - s);
      });

      resets.add(index);
      tables.get(tableIndex)[docIndex] = 0;
      size--;
    }

    // metrics too
    long s = System.nanoTime();
    Map<String, RoaringBitmap> valueMap = indexMap.get(METRICS);
    if (valueMap != null) {
      Iterator<Entry<String, RoaringBitmap>> iterator = valueMap.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry<String, RoaringBitmap> metric = iterator.next();
        RoaringBitmap rr = metric.getValue();
        int sizeInBytes = rr.getSizeInBytes();
        histo[0] += sizeInBytes;
        histo[1]++;
        histo[2] = Math.min(histo[2], sizeInBytes);
        histo[3] = Math.max(histo[3], sizeInBytes);
        for (int i = 0; i < n; i++) {
          int index = indices[i];
          rr.remove(index);
        }
        if (rr.isEmpty()) {
          iterator.remove();
        }
      }
    }
    long metricPurgeLatency = System.nanoTime() - s;
    long totalLatency = System.nanoTime() - start;
    long tagParseLatency = totalLatency - (sortLatency + tagPurgeLatency.get() + metricPurgeLatency);

    logger.info("DocIdBatch remove latency shardId: {} batchSize: {} total latency: {} batch sort/search latency: {} tagPurge latency: {} metricPurge latency: {}  tagParse latency: {} ns metricBitmapSizes: {} docStoreSize: {}",
        shard.getId(), batchSize, totalLatency, sortLatency, tagPurgeLatency.get(), metricPurgeLatency, tagParseLatency, Arrays.toString(histo), size);

  }

  public boolean remove(final long docId, final byte[] tagBuffer, final int offset, final int length) {

    int tableIndex = -1;
    int docIndex = -1;
    boolean docFound = false;
    for (int i = 0; i < tables.size(); i++) {
      long[] table = tables.get(i);
      for (int j = 0; j < table.length; j++) {
        if (table[j] == docId) {
          tableIndex = i;
          docIndex = j;
          docFound = true;
          break;
        }
      }
    }

    if (!docFound) {
      logger.error("Missing meta index for docId: {}", docId);
      return false;
    }

    int index = tableIndex * initialCapacity + docIndex;

    Util.parseTags(tagBuffer, offset, length, (tagKey, tagValue) -> {
      Map<String, RoaringBitmap> valueMap = indexMap.get(tagKey);
      RoaringBitmap bitmap = valueMap.get(tagValue);
      bitmap.remove(index);
      if (bitmap.isEmpty()) {
        valueMap.remove(tagValue);
      }
      if (valueMap.isEmpty()) {
        indexMap.remove(tagKey);
      }
    });

    // metrics too
    Map<String, RoaringBitmap> valueMap = indexMap.get(METRICS);
    if (valueMap != null) {
      Iterator<Entry<String, RoaringBitmap>> iterator = valueMap.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry<String, RoaringBitmap> metric = iterator.next();
        metric.getValue().remove(index);
        if (metric.getValue().isEmpty()) {
          iterator.remove();
        }
      }
    }
    resets.add(index);
    tables.get(tableIndex)[docIndex] = 0;
    size--;
    return true;
  }

  public long[] find(Query query) {
    if (query instanceof MatchAllQuery) {
      long[] result = new long[size];
      readAllKeys(result);
      return result;
    } else {
      RoaringBitmap rr = apply(query);
      return getKeys(rr);
    }
  }

  private void readAllKeys(long[] result) {
    int n = 0;
    for (int tableIndex = 0; tableIndex < tables.size(); tableIndex++) {
      long[] table = tables.get(tableIndex);
      for (int i = 0; i < table.length; i++) {
        long key = table[i];
        if (key != 0) {
          result[n++] = key;
        }
      }
    }
  }

  @Override
  public int search(Query query, String metric, long[] result) throws InSufficientArrayLengthException {

    if (query instanceof MatchAllQuery) {
      if (metaQueryEnabled) {
        Map<String, RoaringBitmap> metrics = indexMap.get(METRICS);
        if (metrics != null) {
          RoaringBitmap metricMap = metrics.get(metric);
          if (metricMap == null) {
            return 0;
          }
          int cardinality = metricMap.getCardinality();
          if (cardinality > result.length) {
            throw new InSufficientArrayLengthException(cardinality, result.length);
          }
          getKeys(metricMap, result);
          return cardinality;
        } else {
          return 0;
        }
      }
      if (size > result.length) {
        throw new InSufficientArrayLengthException(size, result.length);
      }
      logger.warn("Executing a matchall query without a metric.");
      readAllKeys(result);
      return size;
    }

    RoaringBitmap rr = apply(query);
    if (metaQueryEnabled) {
      Map<String, RoaringBitmap> metrics = indexMap.get(METRICS);
      if (metrics != null) {
        RoaringBitmap metricMap = metrics.get(metric);
        if (metricMap == null) {
          return 0;
        }
        rr.and(metricMap);
      } else {
        return 0;
      }
      logger.debug("Querying for metric in new store: {} -> {}", metric, rr.getCardinality());
    }

    int cardinality = rr.getCardinality();
    if (cardinality > result.length) {
      throw new InSufficientArrayLengthException(cardinality, result.length);
    }
    getKeys(rr, result);
    return cardinality;
  }

  private RoaringBitmap apply(Query query) {
    // Note that this method doesn't handle exact match. The caller needs to
    // handle that.
    return query.getFilter().apply(indexMap);
  }

  @Override
  public int size() {
    return size;
  }

  private List<RoaringBitmap> findBitMaps(Filter[] filters) {
    List<RoaringBitmap> bitmaps = new ArrayList<>(filters.length);
    for (int i = 0; i < filters.length; i++) {
      Filter filter = filters[i];
      RoaringBitmap filterRR = new RoaringBitmap(); // empty bitmap

      if (filter instanceof MatchAllFilter) {
        for (Map.Entry<String, Map<String, RoaringBitmap>> entryMap : this.indexMap.entrySet()) {
          final RoaringBitmap fastAgg = FastAggregation.or(entryMap.getValue().values().iterator());
          filterRR = FastAggregation.or(fastAgg, filterRR);
        }
        bitmaps.add(filterRR);
        return bitmaps;
      } else {
        String tagKey = filter.tagKey;
        Map<String, RoaringBitmap> valueMap = this.indexMap.get(tagKey);
        if (null != valueMap) {
          filter.apply(filterRR, valueMap);
        }
        bitmaps.add(filterRR);
      }
    }
    return bitmaps;
  }

  private long[] getKeys(RoaringBitmap rr) {
    long[] result = new long[rr.getCardinality()];
    getKeys(rr, result);
    return result;
  }

  private void getKeys(RoaringBitmap rr, long[] result) {
    int i = 0;
    final PeekableIntIterator iterator = rr.getIntIterator();
    while (iterator.hasNext()) {
      final int index = iterator.next();
      final int tableIndex = index / initialCapacity;
      final int docIndex = index % initialCapacity;
      result[i++] = tables.get(tableIndex)[docIndex];
    }
  }

  public void metaQuery() {
    queryRunner.run();
  }

  /**
   * Singleton query runner for this doc store instance. It references the shard we belong to and
   * has a bunch of containers required (for now) to properly query the system for meta.
   *
   * <p>TODO - Improve this! It's naive and sucks but kinda functional.
   */
  class DocStoreQuery {
    //    final MetaSearchResults shardMetaResults;
    //    final HashTable tagTable;
    final byte[] tagPointerBuf;
    //    final HashTable timeSeriesTable;
    //    final byte[][] timeseriesPointerAndData;
    //    final HashFunction hashFunction;

    // For tag keys OR value counts. <tag key OR value, count>
    Map<String, Integer> singleCounts = Maps.newHashMap();
    // For the BASIC and KEY + VALUE queries. <tagKey, <tagValue, count>>
    Map<String, Map<String, Integer>> multiCounts = Maps.newHashMap();
    // Cache to find out what tag sets we've seen so far.
    // TODO - watch this! If it gets to a certain size we may want to purge it
    // to avoid OOMs.
    TLongObjectMap<Map<String, String>> seenAndParsed = new TLongObjectHashMap<>();
    long exactMatchCount = 0;
    Map<String, RoaringBitmap> matchedMetrics = Maps.newHashMap();
    List<RoaringBitmap> anyMatched = Lists.newArrayList();
    List<RoaringBitmap> anyMatchedNot = Lists.newArrayList();
    byte[] buf = new byte[4096];

    DocStoreQuery() {
      //      shardMetaResults = shard.shardMetaResults();
      //      tagTable = shard.tagTable();
      //      tagPointerBuf = shard.tagTableBuffer();
      //      timeSeriesTable = shard.timeSeriesTable();
      //      timeseriesPointerAndData = shard.timeseriesPointerAndData();
      //      hashFunction = shard.hashFunction();
      tagPointerBuf = new byte[HashTable.valSz];
    }

    /**
     * Executes the query and populates shardMetaResults with the results. We'll try not to scan the
     * whole index but if the user gives us an "AnyFilter" we're screwed.
     *
     * <p>The logic is ugly. Given a filter, we first recursively walk the filter and pull out the
     * metrics or tag keys we want from the store and run the values through the filter set to find
     * anything we want. Metric filters will populate the "matchedMetrics" map.
     *
     * <p>If some tag documents were found but no metric query was present we'll add all of the
     * metrics in the index. Otherwise if the query had a filter for metrics, we'll add just those
     * metric bitmaps to the map.
     *
     * <p>WARNING: If an AnyFilter was found anywhere then we have to walk the whole index so we do
     * that next. If there are not filters we account for them.
     *
     * <p>If we have some metrics and optional tag filters, we'll now run through and AND the tag
     * bitmap with each metric bitmap to find out what docs survive. Matches are then processed
     * depending on the query type which will usually entail a lookup in the tag table and, in the
     * case of an explicit match, a lookup in the time series table to get the tag count.
     *
     * <p>Meta query types in order of best to worst performance are: TIMESERIES - Doesn't need a
     * lot of structures and only resolves the tags up to the container limit. Subsequent docs are
     * just added to the total hits.
     *
     * <p>METRICS - Ditto with the above though we don't even need to hit the tag table UNLESS
     * explicit tags is in effect. Then we lookup the tag and time series.
     *
     * <p>TAG_KEYS || TAG_VALUES - These use the singleCounts and seenAndParsed maps.
     *
     * <p>TAG_KEYS_AND_VALUES || BASIC - These use the multiCounts and seenAndParsed maps. Ug.
     *
     * <p>And the AnyFilter queries are the worse as they will scan the entire index (minus metrics)
     * and use whatever the queries above require as well as the anyMatched and anyMatchedNot lists
     * (though these will be limited to 4096 entries and periodically flattened).
     */
    void run() {
      final MetaQuery query = shardMetaResults.query();
      List<RoaringBitmap> matched = Lists.newArrayList();

      if (!(query.getFilter() instanceof AnyFilter)) {
        filter(query.getFilter(), matched);
      }

      RoaringBitmap tags = null;
      if (!matched.isEmpty()) {
        // if matched wasn't empty then we had a tag filter, otherwise we'll
        // just look at the matching metrics.
        if (query.getFilter().getOperator() == Operator.AND) {
          tags = FastAggregation.and(matched.iterator());
        } else {
          // we OR the nots as well.
          tags = FastAggregation.or(matched.iterator());
        }
        if (!query.hasMetricFilter() && matchedMetrics.isEmpty()) {
          // we copy cause we clear.
          matchedMetrics.putAll(indexMap.get(METRICS));
        }
      } else {
        tags = new RoaringBitmap();
      }

      /**
       * ----------- WARNING ----------- * /* HERE be demons meaning we're scanning the whole index.
       */
      if (query.anyFilter() != null || query.notAnyFilter() != null) {
        if (query.anyFilter() == null && query.notAnyFilter() != null) {
          // edge case
          matchAnyOnlyNots();
        } else {
          matchAny();
        }

        if (anyMatched.size() > 0) {
          if (tags != null) {
            tags = FastAggregation.or(tags, anyMatched.get(0));
          } else {
            tags = anyMatched.get(0);
          }
        }

        if (anyMatchedNot.size() > 0) {
          if (tags == null) {
            // this shouldn't happen. If we only had NOT any's then we'd have
            // picked everything EXCEPT the nots.
          }
          tags.andNot(anyMatchedNot.get(0));
        }
      }
      /** ------------------------------- */
      if (tags.isEmpty() && query.hasTagOrAnyFilter()) {
        // no matches
        cleanup();
        return;
      }

      if (query.hasMetricFilter() && matchedMetrics.isEmpty()) {
        // we had a metric filter but didn't find a match
        cleanup();
        return;
      } else if (matchedMetrics == null || matchedMetrics.isEmpty()) {
        // an edge case where we want to filter tags on ALL metrics.
        matchedMetrics.putAll(indexMap.get(METRICS));
      }

      RoaringBitmap timeSeriesCardinality = new RoaringBitmap();
      for (final Map.Entry<String, RoaringBitmap> metric : matchedMetrics.entrySet()) {
        RoaringBitmap m = null;
        if (tags.isEmpty()) {
          m = metric.getValue();
        } else if (query.getFilter().getOperator() == Operator.NOT) {
          m = metric.getValue().clone();
          m.andNot(tags);
        } else {
          m = FastAggregation.and(metric.getValue(), tags);
        }
        if (m.getCardinality() < 1) {
          continue;
        }

        switch (query.metaQueryType()) {
          case TIMESERIES:
            checkAndAddTimeSeries(m, metric.getKey());
            break;
          case METRICS:
            checkAndAddMetric(m, metric.getKey());
            break;
          case TAG_KEYS:
          case TAG_VALUES:
          case TAG_KEYS_AND_VALUES:
          case BASIC:
            checkAndAddTags(m, metric.getKey(), timeSeriesCardinality);
            break;
          default:
            // no-op
        }
      }

      // post filter ops for some types
      switch (query.metaQueryType()) {
        case TAG_KEYS:
        case TAG_VALUES:
        case TAG_KEYS_AND_VALUES:
        case BASIC:
          stashTagCounts(
              exactMatchCount > 0 ? exactMatchCount : timeSeriesCardinality.getLongCardinality());
          break;
        default:
          // no-op
      }
      cleanup();
    }

    /**
     * Resets containers and counters. Make sure to call this before the next query, ideally after
     * finishing one so the GC can cleanup.
     */
    void cleanup() {
      matchedMetrics.clear();
      singleCounts.clear();
      multiCounts.clear();
      seenAndParsed.clear();
      exactMatchCount = 0;
      anyMatched.clear();
      anyMatchedNot.clear();
    }

    /**
     * The workhorse for meta queries.
     *
     * @param filter
     * @param matched
     */
    public void filter(final Filter filter, final List<RoaringBitmap> matched) {
      /** ------- Chain ------- */
      if (filter instanceof ChainFilter) {
        final ChainFilter chain = (ChainFilter) filter;
        List<RoaringBitmap> notMaps = null;
        switch (chain.getOperator()) {
          case AND:
            List<RoaringBitmap> andMaps = Lists.newArrayList();
            for (final Filter child : chain.getChain()) {

              List<RoaringBitmap> subMaps = Lists.newArrayList();
              if (child.getOperator() == Operator.NOT) {
                if (notMaps == null) {
                  notMaps = Lists.newArrayList();
                }
                filter(child, notMaps);
              } else {
                filter(child, subMaps);
              }
              if (subMaps.size() > 0) {
                andMaps.add(FastAggregation.or(subMaps.iterator()));
              }
            }

            if (andMaps.isEmpty()) {
              if (notMaps != null && !notMaps.isEmpty()) {
                // ug special edge case for tags. This means we want ALL tag keys
                // and values EXCEPT for some nots. For now we walk the metrics
                // and combine all the docs.
                // TODO - IF this is the top level (not nested) we could just
                // pop it up and match on the specific metric(s) we want for a
                // faster match.
                andMaps.add(allMetricDocs());
              } else {
                return;
              }
            }

            RoaringBitmap fastAnd = FastAggregation.and(andMaps.iterator());
            if (fastAnd.getCardinality() < 1) {
              return;
            }
            if (notMaps != null && !notMaps.isEmpty()) {
              RoaringBitmap nt = FastAggregation.or(notMaps.iterator());
              fastAnd.andNot(nt);
            }
            matched.add(fastAnd);
            return;
          case OR:
            List<RoaringBitmap> orMaps = Lists.newArrayList();
            for (final Filter child : chain.getChain()) {
              if (child.getOperator() == Operator.NOT) {
                if (notMaps == null) {
                  notMaps = Lists.newArrayList();
                }
                filter(child, notMaps);
              } else {
                filter(child, orMaps);
              }
            }
            if (orMaps.isEmpty()) {
              return;
            }
            RoaringBitmap fastOr = FastAggregation.or(orMaps.iterator());
            if (fastOr.getCardinality() < 1) {
              return;
            }
            if (notMaps != null && !notMaps.isEmpty()) {
              RoaringBitmap nt = FastAggregation.or(notMaps.iterator());
              fastOr.andNot(nt);
            }
            matched.add(fastOr);
            return;
          default:
            throw new IllegalStateException("Unsupported chain operator: " + chain.getOperator());
        }
        /** ------- Metric Filter ------- */
      } else if (filter instanceof MetricFilter) {
        final Map<String, RoaringBitmap> values = indexMap.get(METRICS);
        if (values == null) {
          // we didn't have any metrics. THAT shouldn't happen
          return;
        }

        if (filter.getOperator() == Operator.NOT && matchedMetrics.isEmpty()) {
          matchedMetrics.putAll(values);
        }

        if (filter.getType() == Type.LITERAL) {
          if (filter.getTagValues().length == 1) {
            // short circuit
            String m = filter.getTagValues()[0];
            RoaringBitmap rr = values.get(m);
            if (rr == null) {
              return;
            }
            if (filter.getOperator() == Operator.NOT) {
              matchedMetrics.remove(m);
            } else {
              RoaringBitmap metricRr = matchedMetrics.get(m);
              if (metricRr == null) {
                matchedMetrics.put(m, rr);
              } else {
                // skip it, another filter picked it up.
              }
            }
          } else {
            for (int i = 0; i < filter.getTagValues().length; i++) {
              String m = filter.getTagValues()[i];
              RoaringBitmap rr = values.get(m);
              if (rr == null) {
                continue;
              }

              if (filter.getOperator() == Operator.NOT) {
                matchedMetrics.remove(m);
              } else {
                RoaringBitmap metricRr = matchedMetrics.get(m);
                if (metricRr == null) {
                  matchedMetrics.put(m, rr);
                } else {
                  // skip it, another filter picked it up.
                }
              }
            }
          }
        } else {
          for (final String metric : values.keySet()) {
            if (filter.matchesPattern(metric)) {
              if (filter.getOperator() == Operator.NOT) {
                matchedMetrics.remove(metric);
                continue;
              }

              RoaringBitmap metricRr = matchedMetrics.get(metric);
              if (metricRr == null) {
                matchedMetrics.put(metric, values.get(metric));
              } else {
                // skip it, another filter picked it up.
              }
            }
          }

          if (filter.getOperator() == Operator.AND) {
          }
        }
        return;
        /** ------- Literal Filter ------- */
      } else if (filter instanceof LiteralFilter) {
        final Map<String, RoaringBitmap> values = indexMap.get(filter.getTagKey());
        if (values == null) {
          return;
        }
        for (int i = 0; i < filter.getTagValues().length; i++) {
          RoaringBitmap rr = values.get(filter.getTagValues()[i]);
          if (rr != null) {
            matched.add(rr);
          }
        }
        return;
        /** ------- Regexp Filter ------- */
      } else if (filter instanceof RegexpFilter) {
        final Map<String, RoaringBitmap> values = indexMap.get(filter.getTagKey());
        if (values == null) {
          return;
        }

        // shortcut
        if (filter.matchAll()) {
          RoaringBitmap rr = FastAggregation.or(values.values().iterator());
          if (rr != null && !rr.isEmpty()) {
            matched.add(rr);
          }
          return;
        }

        for (final String v : values.keySet()) {
          if (filter.matchesPattern(v)) {
            RoaringBitmap rr = values.get(v);
            if (rr != null) {
              matched.add(rr);
            }
          }
        }
        return;
        /** ------- Tag Key ------- */
      } else if (filter instanceof TagKeyFilter) {
        if (filter.getType() == Type.LITERAL) {
          for (int i = 0; i < filter.getTagValues().length; i++) {
            final Map<String, RoaringBitmap> values = indexMap.get(filter.getTagValues()[i]);
            if (values == null || values.isEmpty()) {
              continue;
            }

            matched.addAll(values.values());
          }
        } else {
          for (final String key : indexMap.keySet()) {
            if (key.equals(METRICS)) {
              continue;
            }

            if (filter.matchesPattern(key)) {
              final Map<String, RoaringBitmap> values = indexMap.get(key);
              if (values == null || values.isEmpty()) {
                continue;
              }

              // whew
              for (final RoaringBitmap rr : values.values()) {
                matched.add(rr);
              }
            }
          }
        }
        return;
        /** ------- Any ------- */
      } else if (filter instanceof AnyFilter) {
        // TODO... this, this is evil! We have to iterate over EVERYTHING!
        throw new UnsupportedOperationException("NOT implemented yet.");
        //        // any of the three
        //        boolean matches = false;
        //        if (!Strings.isNullOrEmpty(metric)) {
        //          matches = ((AnyFieldRegexFilter) filter).matches(metric);
        //        }
        //        if (!Strings.isNullOrEmpty(tagKey)) {
        //          matches |= ((AnyFieldRegexFilter) filter).matches(tagKey);
        //        }
        //        return matches | ((AnyFieldRegexFilter) filter).matches(tagValue);
      }
    }

    /**
     * Flattens all of the metrics docs via an OR agg.
     *
     * @return The OR'd docs.
     */
    RoaringBitmap allMetricDocs() {
      Map<String, RoaringBitmap> metrics = indexMap.get(METRICS);
      if (metrics == null) {
        return new RoaringBitmap();
      }
      return FastAggregation.or(metrics.values().iterator());
    }

    /**
     * Runs through the entire index matching on positive AnyFilters and optional any not filters.
     * At the end the anyMatched and AnyMatchedNot lists would have at most one entry.
     */
    void matchAny() {
      final AnyFilter anyFilter = shardMetaResults.query().anyFilter();
      final AnyFilter notAnyFilter = shardMetaResults.query().notAnyFilter();

      for (Map.Entry<String, Map<String, RoaringBitmap>> entry : indexMap.entrySet()) {
        if (entry.getKey().equals(METRICS)) {
          // skipping metrics for any
          // TODO - ?
          continue;
        }

        if (anyFilter != null && anyFilter.match(StringType.TAG_KEY, entry.getKey())) {
          anyMatched.addAll(entry.getValue().values());
        }
        if (notAnyFilter != null && notAnyFilter.match(StringType.TAG_KEY, entry.getKey())) {
          anyMatchedNot.addAll(entry.getValue().values());
        } else {
          // descent into madness.
          for (Map.Entry<String, RoaringBitmap> value : entry.getValue().entrySet()) {
            if (anyFilter != null && anyFilter.match(StringType.TAG_VALUE, value.getKey())) {
              anyMatched.add(value.getValue());
            }
            if (notAnyFilter != null && notAnyFilter.match(StringType.TAG_VALUE, value.getKey())) {
              anyMatchedNot.add(value.getValue());
            }
          }
        }

        if (anyMatched.size() > 4096) {
          orMaps(anyMatched);
        }
        if (anyMatchedNot.size() > 4096) {
          orMaps(anyMatchedNot);
        }
      }

      if (anyMatched.size() > 0) {
        orMaps(anyMatched);
      }
      if (anyMatchedNot.size() > 0) {
        orMaps(anyMatchedNot);
      }
    }

    /**
     * Runs through the entire index, merging anything that failed the NOT filter into the
     * anyMatched list and anything that passed the NOT filter in the anyMatchedNot list. At the end
     * the anyMatched and AnyMatchedNot lists would have at most one entry.
     */
    void matchAnyOnlyNots() {
      final AnyFilter notAnyFilter = shardMetaResults.query().notAnyFilter();

      for (Map.Entry<String, Map<String, RoaringBitmap>> entry : indexMap.entrySet()) {
        if (entry.getKey().equals(METRICS)) {
          // skipping metrics for any
          // TODO - ?
          continue;
        }

        if (notAnyFilter.match(StringType.TAG_KEY, entry.getKey())) {
          // matched on the key so exclude all it's docs
          anyMatchedNot.addAll(entry.getValue().values());
          continue;
        }

        for (Map.Entry<String, RoaringBitmap> value : entry.getValue().entrySet()) {
          if (notAnyFilter.match(StringType.TAG_VALUE, value.getKey())) {
            // matched on the key so exclude all it's docs
            anyMatchedNot.add(value.getValue());
            continue;
          }

          anyMatched.add(value.getValue());
        }

        if (anyMatched.size() > 4096) {
          orMaps(anyMatched);
        }
        if (anyMatchedNot.size() > 4096) {
          orMaps(anyMatchedNot);
        }
      }

      if (anyMatched.size() > 0) {
        orMaps(anyMatched);
      }
      if (anyMatchedNot.size() > 0) {
        orMaps(anyMatchedNot);
      }
    }

    /**
     * Utility to flatten the maps for the matchAny routines. Clears the list and puts the flattned
     * map back as the first entry.
     *
     * @param maps The non-null list of maps to OR.
     */
    void orMaps(final List<RoaringBitmap> maps) {
      RoaringBitmap flat = FastAggregation.or(maps.iterator());
      maps.clear();
      maps.add(flat);
    }

    /**
     * Handles TAG_KEY, TAG_VALUE, TAG_KEY_AND_VALUE and BASIC queries by resolving (and caching)
     * the tag maps.
     *
     * @param metricMap             The metric map.
     * @param metric                The metric name.
     * @param timeSeriesCardinality The tag cardinality to OR with the metric to track the total
     *                              cardinality..
     */
    void checkAndAddTags(
        final RoaringBitmap metricMap, final String metric, RoaringBitmap timeSeriesCardinality) {
      timeSeriesCardinality.or(metricMap);
      final PeekableIntIterator it = metricMap.getIntIterator();
      long mHash = hashFunction.hash(metric.getBytes(StandardCharsets.UTF_8));
      while (it.hasNext()) {
        final int index = it.next();
        final int tableIndex = index / initialCapacity;
        final int docIndex = index % initialCapacity;
        final long docId = tables.get(tableIndex)[docIndex];

        if (shardMetaResults.query().isExactMatch()) {
          final long tsKey = hashFunction.update(mHash, docId);
          byte actualTagCount = shard.getTagCount(tsKey);
          if (actualTagCount != shard.NOT_FOUND
              && actualTagCount != shardMetaResults.query().getTagCount()) {
            // skip
            continue;
          } // else it's already gone.
        }

        // check our cache
        Map<String, String> extant = seenAndParsed.get(docId);
        if (extant != null) {
          incTagCounts(extant);
          continue;
        }

        // cache miss. We need to parse and stash it.
        if (!shard.getTagPointer(docId, tagPointerBuf)) {
          continue;
        }
        long tagAddress = ByteArrays.getLong(tagPointerBuf, 0);
        int tagLength = ByteArrays.getInt(tagPointerBuf, 8);
        if (tagLength >= buf.length) {
          buf = new byte[tagLength];
        }
        Memory.read(tagAddress, buf, tagLength);

        // parse and cache
        extant = Util.createTagMap(buf, 0, tagLength);

        seenAndParsed.put(docId, extant);
        incTagCounts(extant);
      }
    }

    /**
     * Handles METRICS queries.
     *
     * @param metricMap The matched documents.
     * @param metric    The metric name.
     */
    void checkAndAddMetric(RoaringBitmap metricMap, String metric) {
      final MetaQuery query = shardMetaResults.query();
      if (shardMetaResults.query().isExactMatch()) {
        // OH say it ain't so!!! This sucks as we have to lookup the tag counts
        // for EVERY series we matched. Ugg.
        final long mHash = hashFunction.hash(metric.getBytes(StandardCharsets.UTF_8));
        final int expectedTagCount = shardMetaResults.query().getTagCount();
        int cardinality = 0;
        final PeekableIntIterator it = metricMap.getIntIterator();
        while (it.hasNext()) {
          final int index = it.next();
          final int tableIndex = index / initialCapacity;
          final int docIndex = index % initialCapacity;
          final long docId = tables.get(tableIndex)[docIndex];
          final long tsKey = hashFunction.update(mHash, docId);
          byte actualTagCount = shard.getTagCount(tsKey);
          if (actualTagCount != shard.NOT_FOUND) {
            if (actualTagCount == expectedTagCount) {
              // matched
              cardinality++;
            }
          } // else it's already gone.
        }

        if (!query.canAdd()) {
          shardMetaResults.incrementHits(cardinality);
          return;
        }
        shardMetaResults.addString(metric, cardinality);
        return;
      }

      if (!query.canAdd()) {
        shardMetaResults.incrementHits(metricMap.getCardinality());
        return;
      }
      shardMetaResults.addString(metric, metricMap.getCardinality());
    }

    /**
     * Handles TIMESERIES queries wherein we return the whole series.
     *
     * @param metricMap The metric documents.
     * @param metric    The metric name.
     */
    void checkAndAddTimeSeries(RoaringBitmap metricMap, String metric) {
      final MetaQuery query = shardMetaResults.query();
      if (shardMetaResults.query().isExactMatch()) {
        // OH say it ain't so!!! This sucks as we have to lookup the tag counts
        // for EVERY series we matched. Ugg.
        final long mHash = hashFunction.hash(metric.getBytes(StandardCharsets.UTF_8));
        final int expectedTagCount = shardMetaResults.query().getTagCount();
        int cardinality = 0;
        final PeekableIntIterator it = metricMap.getIntIterator();
        boolean outOfSpace = false;
        int countIndex = -1;
        int written = 0;
        while (it.hasNext()) {
          final int index = it.next();
          final int tableIndex = index / initialCapacity;
          final int docIndex = index % initialCapacity;
          final long docId = tables.get(tableIndex)[docIndex];
          final long tsKey = hashFunction.update(mHash, docId);
          byte actualTagCount = shard.getTagCount(tsKey);
          if (actualTagCount != shard.NOT_FOUND) {
            if (actualTagCount == expectedTagCount) {
              // matched
              cardinality++;
              if (outOfSpace) {
                continue;
              }

              if (query.canAdd()) {
                if (countIndex < 0) {
                  countIndex = shardMetaResults.addTimeSeries(metric, 1);
                }

                if (!shard.getTagPointer(docId, tagPointerBuf)) {
                  // throw new IllegalStateException("NO DOC!!!!! for" + docId);
                  logger.error("******** NO DOC FOR: " + docId);
                } else {
                  long tagAddress = ByteArrays.getLong(tagPointerBuf, 0);
                  int tagLength = ByteArrays.getInt(tagPointerBuf, 8);
                  shardMetaResults.addTimeSeriesTags(tagAddress, tagLength);
                  written++;
                }
              } else {
                outOfSpace = true;
              }
            }
          } // else it's already gone.
        }

        if (cardinality > 0) {
          shardMetaResults.incrementHits(cardinality - (written > 0 ? 1 : 0));
        }

        if (written > 1) {
          shardMetaResults.updateCount(countIndex, written);
        }
        return;
      }

      // it's not an exact match so we can figure out how many results we
      // can add then copy only those that will fit.
      int remaining = query.sizeRemaining();
      if (remaining == 0) {
        // can't add so just increment the hits
        shardMetaResults.incrementHits(metricMap.getCardinality());
        return;
      }

      // we need to iterate and checkout some slots
      int i = 0;
      for (; i < metricMap.getCardinality(); i++) {
        if (!query.canAdd()) {
          break;
        }
      }

      if (i < 1) {
        // lost the race so we can't add so just increment the hits
        shardMetaResults.incrementHits(metricMap.getCardinality());
        return;
      }

      // we successfully checked out a few so add em
      shardMetaResults.addTimeSeries(metric, i);
      final PeekableIntIterator it = metricMap.getIntIterator();
      for (int a = 0; a < i; a++) {
        final int index = it.next();
        final int tableIndex = index / initialCapacity;
        final int docIndex = index % initialCapacity;
        final long docId = tables.get(tableIndex)[docIndex];
        if (!shard.getTagPointer(docId, tagPointerBuf)) {
          // throw new IllegalStateException("NO DOC!!!!! for" + docId);
          logger.error("******** NO DOC FOR: " + docId);
        } else {
          long tagAddress = ByteArrays.getLong(tagPointerBuf, 0);
          int tagLength = ByteArrays.getInt(tagPointerBuf, 8);
          shardMetaResults.addTimeSeriesTags(tagAddress, tagLength);
        }
      }
    }

    /**
     * Increments the tag key and value counts where appropriate. TODO - auto-boxed integers are
     * BAD!! Find a better structure.
     *
     * @param extant The tag set to increment counters for.
     */
    void incTagCounts(Map<String, String> extant) {
      final MetaQuery query = shardMetaResults.query();
      if (query.isExactMatch()) {
        exactMatchCount++;
      }
      // yay, save the parsing!
      for (Map.Entry<String, String> entry : extant.entrySet()) {
        if (query.metaAggregationField() != null
            && !query.metaAggregationField().equals(entry.getKey())) {
          // skip
          continue;
        }

        if (query.metaQueryType() == MetaQueryType.TAG_KEYS) {
          Integer count = singleCounts.get(entry.getKey());
          if (count == null) {
            singleCounts.put(entry.getKey(), 1);
          } else {
            singleCounts.put(entry.getKey(), count + 1);
          }
        } else if (query.metaQueryType() == MetaQueryType.TAG_VALUES) {
          Integer count = singleCounts.get(entry.getValue());
          if (count == null) {
            singleCounts.put(entry.getValue(), 1);
          } else {
            singleCounts.put(entry.getValue(), count + 1);
          }
        } else {
          Map<String, Integer> vals = multiCounts.get(entry.getKey());
          if (vals == null) {
            vals = Maps.newHashMap();
            vals.put(entry.getValue(), 1);
            multiCounts.put(entry.getKey(), vals);
          } else {
            Integer count = vals.get(entry.getValue());
            if (count == null) {
              vals.put(entry.getValue(), 1);
            } else {
              vals.put(entry.getValue(), count + 1);
            }
          }
        }
      }
    }

    /**
     * Flushes the tags and counts to the shard results container.
     *
     * @param timeSeriesCardinality The total cardinality to record.
     */
    void stashTagCounts(long timeSeriesCardinality) {
      boolean canAdd = true; // saved here to avoid memory lookups in the atomic
      switch (shardMetaResults.query().metaQueryType()) {
        case TAG_KEYS:
        case TAG_VALUES:
          for (final Map.Entry<String, Integer> entry : singleCounts.entrySet()) {
            if (canAdd) {
              canAdd = shardMetaResults.query().canAdd();
              if (canAdd) {
                shardMetaResults.addString(entry.getKey(), entry.getValue(), false);
                continue;
              }
            }

            shardMetaResults.incrementHits(entry.getValue());
          }
        case TAG_KEYS_AND_VALUES:
        case BASIC:
          for (final Map.Entry<String, Map<String, Integer>> key : multiCounts.entrySet()) {
            long cardinality = 0;
            for (Map.Entry<String, Integer> value : key.getValue().entrySet()) {
              cardinality += value.getValue();
            }

            if (canAdd) {
              // see if we can add all results, otherwise we don't want to return
              // partials
              int success = 0;
              for (int i = 0; i < key.getValue().size(); i++) {
                if (shardMetaResults.query().canAdd()) {
                  success++;
                } else {
                  break;
                }
              }
              if (success == key.getValue().size()) {
                // nice, we can add em
                shardMetaResults.addPairKey(key.getKey(), cardinality, key.getValue().size());
                for (Map.Entry<String, Integer> value : key.getValue().entrySet()) {
                  shardMetaResults.addPairValue(value.getKey(), value.getValue());
                }
                continue;
              } else {
                canAdd = false;
              }
            }

            //          // just add the hits
            //          shardMetaResults.incrementHits(cardinality);
          }
          break;
        default:
          // no-op
      }

      shardMetaResults.incrementHits(timeSeriesCardinality);
    }
  }

}
