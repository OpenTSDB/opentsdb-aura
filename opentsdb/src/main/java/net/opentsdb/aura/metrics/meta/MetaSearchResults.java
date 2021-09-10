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
import com.google.common.reflect.TypeToken;
import net.opentsdb.aura.metrics.meta.MetaQueryBuilder.MetaQueryType;
import net.opentsdb.collections.UnsafeHelper;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.meta.MetaDataStorageResult;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.UniqueKeyPair;
import sun.misc.Unsafe;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OK so this is a super fun happy class that's INCREDIBLY ugly but we're trying
 * to be fairly efficient. To use this, a meta query instantiates or grabs this
 * out of a pool and hands it to the TimeSeriesStorage to query the shards.
 * Each shard has it's own instance of this so we re-use the buffers. Each shard
 * runs the query and populates it's own instance with the proper results. When
 * the result is done, it's {@link #merge(MetaSearchResults)}'d into the shared
 * object. Then after the shards are done we can {@link #sort()}.
 *
 * Merging has to be done synchronized and it is ugly as any query type other than
 * TimeSeries requires searching and merging. TimeSeries queries just append
 * since each is unique across shards.
 *
 * The buffer has multiple uses and layouts depending on the meta query response
 * type and works with the string array (we don't copy strings into the buffer
 * as they're already present in the DocStore maps.)
 *
 * TIMESERIES:
 * In this case the string array contains a list of metric names and the buffer
 * will contain the count of series for each metric followed by the off-heap
 * address and length for each tag set.
 *
 * METRICS:
 * TAG_KEYS:
 * TAG_VALUES:
 * The string array has the metric or tag key or value strings and the buffer
 * just has the cardinality for each. Easy peasy.
 *
 * TAG_KEYS_AND_VALUES:
 * BASIC:
 * The strings array has the tag key followed by each unique tag value. The
 * buffer will have the key cardinality, then the count of values following the
 * key, then for each value, the cardinality.
 *
 * TODO - We'll need longs for the cardinality eventually which means we'll need
 * the long cardinality in the RoarinBitmaps.
 *
 * TODO - many opportunities for optimizations here. E.g. for time series if we
 * share tag sets across a lot of metrics, we don't need to store each ref.
 * (though a lot of times we're looking at many tags per metric so we do it
 * this way for now).
 *
 * TODO - Also find some way to get consistent results from shards. Right now
 * it's completely indeterminate who will hit the size limit first or how many
 * and in what order the results will be.
 *
 * TODO - use longs instead of ints for hits, counts, etc.
 */
public class MetaSearchResults implements MetaDataStorageResult, SharedMetaResult {

  private MetaQuery query;
  private byte[] buffer;
  private String[] strings;
  private int stringsIdx;
  private int writeIndex;
  private int size;
  private int hits;
  private int tagPairs;
  private byte[] decode_buffer;
  private String[] sortStrings;
  private byte[] sortBuffer;

  public MetaSearchResults() {
    buffer = new byte[4096];
    decode_buffer = new byte[4096];
    strings = new String[4096];
  }

  /**
   * <b>NOTE</b> Call this BEFORE sending the shard to the query system or using
   * it. Otherwise we won't be able to process the query.
   * @param query The query to associate with this run.
   */
  @Override
  public void reset(MetaQuery query) {
    stringsIdx = 0;
    writeIndex = 0;
    size = 0;
    hits = 0;
    tagPairs = 0;
    this.query = query;
  }

  /** @return The query associated with this result set. */
  @Override
  public MetaQuery query() {
    return query;
  }

  /**
   * Increments only the total hits. Use this after we've hit the size limit
   * and need to keep track of the total documents.
   * @param amount The amount of documents to increment.
   */
  public void incrementHits(final long amount) {
    hits += amount;
  }

  /**
   * Called for TIMESERIES searches and will append the metric name in the
   * strings array and size the buffer for the number of tag sets we'll return.
   * Call {@link #addTimeSeriesTags(long, int)} next {@code count} number of times.
   * @param metric The metric to add.
   * @param count The number of tag sets we'll be adding.
   * @return The index of the count so we can update it later.
   */
  @Override
  public int addTimeSeries(final String metric, int count) {
    if (count == 0) {
      return 0;
    }

    int countIndex = writeIndex;
    // tags count, tag addr, tag len, tag addr, tag len...
    require(4 + (count * (8 + 4)));
    Bytes.setInt(buffer, count, writeIndex);
    writeIndex += 4;

    if (stringsIdx >= strings.length) {
      String[] temp = new String[strings.length * 2];
      System.arraycopy(strings, 0, temp, 0, strings.length);
      strings = temp;
    }
    strings[stringsIdx++] = metric;
    hits += count;
    return countIndex;
  }

  /**
   * Called <i>AFTER</i> {@link #addTimeSeries(String, int)} with each tag set
   * address and length.
   * @param addr The off-heapt address to read from.
   * @param len The length of data
   */
  @Override
  public void addTimeSeriesTags(long addr, int len) {
    //require(12); already sized by addTimeSeries();

    Bytes.setLong(buffer, addr, writeIndex);
    writeIndex += 8;

    Bytes.setInt(buffer, len, writeIndex);
    writeIndex += 4;
    size++;
  }

  /**
   * Called when we're updated TAG KEYS or VALUES and we need to increment the
   * total count found at the index returned by {@link #addString(String, int, boolean)}.
   * @param countIndex The index into the buffer to update.
   * @param count The count to write over the existing value.
   */
  @Override
  public void updateCount(int countIndex, int count) {
    if (count == 0) {
      return;
    }
    Bytes.setInt(buffer, count, countIndex);
  }

  /**
   * Adds a string to the strings list and tracks the cardinality in the buffer.
   * Increments the hits by the cardinality. Used for METRICS, TAG KEYS and TAG
   * VALUES queries.
   * @param string The string to add.
   * @param cardinality The cardinality to set.
   * @return The index into the buffer of the count for this string.
   */
  @Override
  public int addString(final String string, int cardinality) {
    return addString(string, cardinality, true);
  }

  /**
   * Adds a string to the strings list and tracks the cardinality in the buffer.
   * @param string The string to add. Used for METRICS, TAG KEYS and TAG VALUES
   * queries.
   * @param cardinality The cardinality to set.
   * @param incrementHits Whether or not to increment the total hits by the
   * cardinality.
   * @return
   */
  @Override
  public int addString(final String string, int cardinality, boolean incrementHits) {
    int countIndex = writeIndex;
    // buffer has counts as ints indexed on the strings index.
    require(4);
    Bytes.setInt(buffer, cardinality, writeIndex);
    writeIndex += 4;

    if (stringsIdx >= strings.length) {
      String[] temp = new String[strings.length * 2];
      System.arraycopy(strings, 0, temp, 0, strings.length);
      strings = temp;
    }

    strings[stringsIdx++] = string;
    size++;
    if (incrementHits) {
      hits += cardinality;
    }
    return countIndex;
  }

  /**
   * Used for TAG_KEYS_AND_VALUES or BASIC queries to add the tag key for an
   * upcoming list of tag values via {@link #addPairValue(String, int)}. Call
   * this first with the number of values we'll add next.
   * @param key the tag key.
   * @param cardinality the cardinality of this key.
   * @param count number of values we'll add
   */
  @Override
  public void addPairKey(final String key, final long cardinality, final int count) {
    // for strings we need 1 + count
    while (stringsIdx + count + 1 >= strings.length) {
      String[] temp = new String[strings.length * 2];
      System.arraycopy(strings, 0, temp, 0, stringsIdx);
      strings = temp;
    }
    strings[stringsIdx++] = key;

    // for the buffer we'll do key card, count, card for each value
    require(4 + 4 + (4 * count));
    Bytes.setInt(buffer, (int) cardinality, writeIndex);
    writeIndex += 4;

    Bytes.setInt(buffer, count, writeIndex);
    writeIndex += 4;
    size += count + 1;
    tagPairs++;
  }

  /**
   * Called <i>AFTER</i> calling {@link #addPairKey(String, long, int)} for each
   * tag value associated with that key.
   * @param value The tag value string to add.
   * @param cardinality The cardinality of that string.
   */
  @Override
  public void addPairValue(final String value, final int cardinality) {
    strings[stringsIdx++] = value;
    Bytes.setInt(buffer, cardinality, writeIndex);
    writeIndex += 4;
  }

  /** @return The number of entries in this result set. It can be the number of
   * tag keys/values or metrics, the number of time series or number of tag keys
   * and values in the set. */
  public int size() {
    return size;
  }

  /**
   * Called with the results from each individual shard to merge the results into
   * a single view to return to the caller. We do this cause each shard is
   * single threaded and we want to re-use the buffer from these results so we
   * have to copy into a shared container that we'll serialize for the user.
   *
   * Depending on the query type this function does different things. E.g. for
   * a TIMESERIES query we simply append all of the series from the other shards
   * to the local buffer. Easy.
   *
   * For METRICS, TAG KEY and TAG VALUE queries we have to search the string
   * array to see if another shard had some of the same results, and if so we
   * increment the cardinality. This is inefficient right now.
   *
   * For TAG_KEYS_AND_VALUES/BASIC we have a nasty double search in that we have
   * to look for the tag keys, THEN we have to look for the values associated
   * with that key. We could try using maps here but this is a memory efficient
   * implementation and we have some memory restraints.
   *
   * TODO - some of this is pretty nasty and inefficient, particularly the string
   * searching. Maybe sort the those first?
   *
   * TODO - maybe buffer off-heap?
   *
   * @param results The shard results to merge.
   */
  public synchronized void merge(final MetaSearchResults results) {
    if (results.size() < 1) {
      return;
    }

    switch (query.metaQueryType()) {
      case TIMESERIES:
        if (results.size() == 0) {
          hits += results.hits;
          return;
        }

        if (stringsIdx < 1) {
          // straight copy of everything.
          growStringsBuffer(results.stringsIdx);
          System.arraycopy(results.strings, 0, strings, 0, results.stringsIdx);
          stringsIdx = results.stringsIdx;

          require(results.writeIndex);
          System.arraycopy(results.buffer, 0, buffer, 0, results.writeIndex);
          writeIndex = results.writeIndex;

          size = results.size;
          hits = results.hits;
          return;
        }

        growStringsBuffer(results.stringsIdx);
        System.arraycopy(results.strings, 0, strings, stringsIdx, results.stringsIdx);
        stringsIdx += results.stringsIdx;

        int readIndex = 0;
        while (readIndex < results.writeIndex) {
          int count = Bytes.getInt(results.buffer, readIndex);
          require(4 + (count * 12));
          Bytes.setInt(buffer, count, writeIndex);
          readIndex += 4;
          writeIndex += 4;

          System.arraycopy(results.buffer, readIndex, buffer, writeIndex, count * 12);
          readIndex += count * 12;
          writeIndex += count * 12;
          size += count;
        }
        hits += results.hits;
        break;
      case METRICS:
      case TAG_KEYS:
      case TAG_VALUES:
        // YEAH it's ugly as all get-out but for now we can try it to avoid the
        // boxing and other ops required to use a map.
        if (results.stringsIdx == 0) {
          // straight copy into an empty container.
          if (results.stringsIdx >= strings.length) {
            strings = new String[results.stringsIdx];
          }
          System.arraycopy(results.strings, 0, strings, 0, results.stringsIdx);

          require(results.stringsIdx * 4);
          System.arraycopy(results.buffer, 0, buffer, 0, results.writeIndex);
          size = results.size;
          hits = results.hits;
        } else {
          StringIterator:
          for (int i = 0; i < results.stringsIdx; i++) {
            String other = results.strings[i];
            int otherCardinality = Bytes.getInt(results.buffer, i * 4);

            for (int x = 0; x < stringsIdx; x++) {
              if (strings[x].equals(other)) {
                int cardinality = Bytes.getInt(buffer, x * 4) + otherCardinality;
                Bytes.setInt(buffer, cardinality, x * 4);
                continue StringIterator;
              }
            }

            // new one
            if (stringsIdx >= strings.length) {
              growStringsBuffer(1);
            }
            strings[stringsIdx++] = other;

            require(4);
            Bytes.setInt(buffer, otherCardinality, writeIndex);
            writeIndex += 4;
            size++;
          }
          hits += results.hits;
        }
        break;
      case TAG_KEYS_AND_VALUES:
      case BASIC:
        // TODO - more testing please.
        if (stringsIdx <= 0) {
          // clone
          growStringsBuffer(results.stringsIdx);
          System.arraycopy(results.strings, 0, strings, 0, results.stringsIdx);
          stringsIdx = results.stringsIdx;

          require(results.writeIndex);
          System.arraycopy(results.buffer, 0, buffer, 0, results.writeIndex);
          writeIndex = results.writeIndex;

          size = stringsIdx;
          hits = results.hits;
          tagPairs = results.tagPairs;
          return;
        }

        int remoteStringIdx = 0;
        int remoteBufferIdx = 0;
        RemoteIterator:
        while (remoteStringIdx < results.stringsIdx) {
          int remoteKeyCard = Bytes.getInt(results.buffer, remoteBufferIdx);
          remoteBufferIdx += 4;

          int remoteCount = Bytes.getInt(results.buffer, remoteBufferIdx);
          remoteBufferIdx += 4;

          final String remoteKey = results.strings[remoteStringIdx++];
          // search just the keys
          int stringSearchIndex = 0;
          int bufferSearchIndex = 0;
          while (stringSearchIndex < stringsIdx) {
            bufferSearchIndex += 4; // skip cardinality
            int count = Bytes.getInt(buffer, bufferSearchIndex);

            if (strings[stringSearchIndex].equals(remoteKey)) {
              // matched!
              // read old card
              int card = Bytes.getInt(buffer, bufferSearchIndex - 4);
              card += remoteKeyCard;
              Bytes.setInt(buffer, card, bufferSearchIndex - 4);

              // now merge the values. If we find one, yay, easy. BUT if we don't
              // then things get ugly.
              List<Object> adds = null; // TODO, string, int, string, int....
              OuterLoop:
              for (int i = remoteStringIdx; i < remoteStringIdx + remoteCount; i++) {
                for (int x = stringSearchIndex + 1; x < stringSearchIndex + 1 + count; x++) {
                  if (results.strings[i].equals(strings[x])) {
                    // matched!
                    int remoteValCard = Bytes.getInt(results.buffer, remoteBufferIdx + ((i - remoteStringIdx) * 4));
                    int localIdx = bufferSearchIndex + 4 + ((x - stringSearchIndex - 1) * 4);
                    int localValCard = Bytes.getInt(buffer, localIdx);
                    Bytes.setInt(buffer, localValCard + remoteValCard, localIdx);
                    continue OuterLoop;
                  }
                }

                // not found, append at end
                if (adds == null) {
                  adds = Lists.newArrayList();
                }
                adds.add(results.strings[i]);
                int remoteCardIdx = remoteBufferIdx + ((i - remoteStringIdx) * 4);
                int remoteValCard = Bytes.getInt(results.buffer, remoteCardIdx);
                adds.add((Integer) remoteValCard);
              } // end OuterLoop:

              // blech, we need to insert some values.
              if (adds != null && adds.size() > 0) {
                int countIdx = bufferSearchIndex;
                int toAdd = adds.size() / 2;
                growStringsBuffer(toAdd);
                require(toAdd * 4);

                // shift
                System.arraycopy(strings, stringSearchIndex + 1 + count, strings, stringSearchIndex + 1 + count + toAdd, stringsIdx - stringSearchIndex + 1 + count);
                stringsIdx += toAdd;

                int start = bufferSearchIndex + 4 + (count * 4);
                int shift = bufferSearchIndex + 4 + (count * 4) + (toAdd * 4);
                System.arraycopy(buffer, start, buffer, shift, writeIndex - start);
                writeIndex += toAdd * 4;

                // copy in
                stringSearchIndex += 1 + count;
                bufferSearchIndex += 4 + (count * 4);
                for (int x = 0; x < adds.size(); x += 2) {
                  strings[stringSearchIndex++] = (String) adds.get(x);
                  Bytes.setInt(buffer, (Integer) adds.get(x + 1), bufferSearchIndex);
                  bufferSearchIndex += 4;
                }
                count += toAdd;
                Bytes.setInt(buffer, count, countIdx);
              }

              remoteStringIdx += remoteCount;
              remoteBufferIdx += (remoteCount * 4);
              continue RemoteIterator;
            }

            // skip
            stringSearchIndex += 1 + count;
            bufferSearchIndex += 4 + (count * 4);
          }

          // not found so append it.
          growStringsBuffer(1 + remoteCount);

          System.arraycopy(results.strings, remoteStringIdx - 1, strings, stringsIdx, 1 + remoteCount);
          stringsIdx += 1 + remoteCount;

          tagPairs += remoteCount;

          require(4 + 4 + (4 * remoteCount));
          int start = remoteBufferIdx - 4 - 4;
          int length = (remoteCount * 4) + 4 + 4;
          System.arraycopy(results.buffer, start, buffer, writeIndex, length);
          writeIndex += length;

          remoteStringIdx += remoteCount;
          remoteBufferIdx += (remoteCount * 4);
        }
        size = stringsIdx;
        hits += results.hits;
    } // end switch
  }

  /**
   * TODO !
   */
  public void sort() {
    // TODO (and make sure this is AFTER we release the shard threads)
    if (size < 1) {
      return;
    }

    switch (query.metaQueryType()) {
      case METRICS:
      case TAG_KEYS:
      case TAG_VALUES:
        // easy one's first
        sortSingleStrings(0, stringsIdx - 1);
        break;
      case TIMESERIES:
        // Prolly the worst since we have to read the strings. Sort on metric then tags
        // TODO - Implement
        break;
      case TAG_KEYS_AND_VALUES:
      case BASIC:
        // messy case.
        sortKeysAndValues();
        break;
    }
  }

  @Override
  public String id() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long totalHits() {
    return hits;
  }

  @Override
  public MetaResult result() {
    return size > 0 ? MetaResult.DATA : MetaResult.NO_DATA;
  }

  @Override
  public Throwable exception() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<String> namespaces() {
    // TODO Auto-generated method stub
    return Collections.emptyList();
  }

  @Override
  public Collection<TimeSeriesId> timeSeries() {
    return query.metaQueryType() == MetaQueryType.TIMESERIES ?
            new TimeSeriesCollection() : Collections.emptySet();
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public Collection<UniqueKeyPair<String, Long>> metrics() {
    return query.metaQueryType() == MetaQueryType.METRICS ?
            new SingleStrings() : Collections.emptyList();
  }

  @Override
  public Map<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> tags() {
    return query.metaQueryType() == MetaQueryType.TAG_KEYS_AND_VALUES ||
            query.metaQueryType() == MetaQueryType.BASIC
            ? new TagKeyAndValuesStrings() : Collections.emptyMap();
  }

  @Override
  public Collection<UniqueKeyPair<String, Long>> tagKeysOrValues() {
    if (query.metaQueryType() == MetaQueryType.TAG_KEYS ||
            query.metaQueryType() == MetaQueryType.TAG_VALUES) {
      return new SingleStrings();
    }
    return Collections.emptyList();
  }

  /**
   * Grows the buffer if we need to.
   * @param length The length we need.
   */
  private void require(int length) {
    while (writeIndex + length >= buffer.length) {
      byte[] temp = new byte[buffer.length * 2];
      System.arraycopy(buffer, 0, temp, 0, writeIndex);
      buffer = temp;
    }
  }

  /**
   * Grows the decode buffer if needed.
   * @param length The length we need.
   */
  private void requireDecodeBuffer(int length) {
    while (length >= decode_buffer.length) {
      decode_buffer = new byte[decode_buffer.length * 2];
    }
  }

  /**
   * Grows the string buffer if needed.
   * @param length The length we need.
   */
  private void growStringsBuffer(int length) {
    while (stringsIdx + length >= strings.length) {
      String[] temp = new String[strings.length * 2];
      System.arraycopy(strings, 0, temp, 0, stringsIdx);
      strings = temp;
    }
  }

  private void sortSingleStrings(int low, int high) {
    if (low < high) {
      int partition = singleStringsPartition(low, high);
      sortSingleStrings(low, partition - 1);
      sortSingleStrings(partition + 1, high);
    }
  }

  private int singleStringsPartition(int low, int high) {
    String pivot = strings[high];
    int i = low;
    for (int j = low; j < high; j++) {
      if (strings[j].compareTo(pivot) < 0) {
        // swap
        String temp = strings[i];
        strings[i] = strings[j];
        strings[j] = temp;

        int count = Bytes.getInt(buffer, i * 4);
        int tempCount = Bytes.getInt(buffer, j * 4);
        Bytes.setInt(buffer, tempCount, i * 4);
        Bytes.setInt(buffer, count, j * 4);
        i++;
      }
    }

    // swap arr[i+1] and arr[high] (or pivot)
    String stringTemp = strings[i];
    strings[i] = strings[high];
    strings[high] = stringTemp;

    int countTemp1 = Bytes.getInt(buffer, i * 4);
    int countTemp2 = Bytes.getInt(buffer, high * 4);
    Bytes.setInt(buffer, countTemp2, i * 4);
    Bytes.setInt(buffer, countTemp1, high * 4);
    return i;
  }

  private void sortKeysAndValues() {
    // remember: Strings: key, values..., key, values...
    //           buffer:  k card, v count, (v card, v card..), k card, v count.....
    int bufferIdx = 0;
    for (int i = 0; i < stringsIdx;) {
      int count = Bytes.getInt(buffer, bufferIdx + 4);
      sortValues(bufferIdx + 8, i + 1, i + 1, i + count - 1);

      // look back and sort keys
      int buffLen = 4 + 4 + (count * 4);
      int stringLen = count + 1;
      int sortIdx = 0;
      for (int x = 0; x < i;) {
        int sortCount = Bytes.getInt(buffer, sortIdx + 4);
        boolean sorted = false;
        if (strings[i].compareTo(strings[x]) < 0) {
          // move it!
          if (sortStrings == null || sortStrings.length < stringLen) {
            sortStrings = new String[stringLen];
          }
          if (sortBuffer == null || sortBuffer.length < buffLen) {
            sortBuffer = new byte[buffLen];
          }

          System.arraycopy(strings, i, sortStrings, 0, stringLen);
          System.arraycopy(buffer, bufferIdx, sortBuffer, 0, buffLen);

          // shift
          System.arraycopy(strings, x, strings, x + stringLen, i - x);
          System.arraycopy(buffer, sortIdx, buffer, sortIdx + buffLen, bufferIdx - sortIdx);

          // insert
          System.arraycopy(sortStrings, 0, strings, x, stringLen);
          System.arraycopy(sortBuffer, 0, buffer, sortIdx, buffLen);
          sorted = true;
        }

        sortIdx += 4 + 4 + (sortCount * 4);
        x += sortCount + 1;
        if (sorted) {
          break;
        }
      }

      bufferIdx += buffLen;
      i += count + 1;
    }

  }

  private void sortValues(int bufferOffset, int baseStringOffset, int low, int high) {
    if (low < high) {
      int partition = partitionValues(bufferOffset, baseStringOffset, low, high);
      sortValues(bufferOffset, baseStringOffset, low, partition - 1);
      sortValues(bufferOffset, baseStringOffset, partition + 1, high);
    }
  }

  private int partitionValues(int bufferOffset, int baseStringOffset, int low, int high) {
    String pivot = strings[high];
    int i = low;
    for (int j = low; j < high; j++) {
      if (strings[j].compareTo(pivot) < 0) {
        // swap
        String temp = strings[i];
        strings[i] = strings[j];
        strings[j] = temp;

        int count = Bytes.getInt(buffer, bufferOffset + ((i - baseStringOffset) * 4));
        int tempCount = Bytes.getInt(buffer, bufferOffset + ((j - baseStringOffset) * 4));
        Bytes.setInt(buffer, tempCount, bufferOffset + ((i - baseStringOffset) * 4));
        Bytes.setInt(buffer, count, bufferOffset + ((j - baseStringOffset) * 4));
        i++;
      }
    }

    // swap arr[i+1] and arr[high] (or pivot)
    String stringTemp = strings[i];
    strings[i] = strings[high];
    strings[high] = stringTemp;

    int countTemp1 = Bytes.getInt(buffer, bufferOffset + ((i - baseStringOffset) * 4));
    int countTemp2 = Bytes.getInt(buffer, bufferOffset + ((high - baseStringOffset) * 4));
    Bytes.setInt(buffer, countTemp2, bufferOffset + ((i - baseStringOffset) * 4));
    Bytes.setInt(buffer, countTemp1, bufferOffset + ((high - baseStringOffset) * 4));
    return i;
  }

  /**
   * An iterator used to view the buffers for TIMESERIES queries. Only one tag set
   * is read off-heap at a time to avoid keeping it all in memory.
   */
  class TimeSeriesCollection implements Collection<TimeSeriesId> {

    class It implements Iterator<TimeSeriesId>, TimeSeriesId, TimeSeriesStringId {
      int metricIdx;
      int bufferIdx;
      int seriesCount;
      int seriesRead;
      Map<String, String> tags;

      It() {
        tags = Maps.newHashMap();
        seriesCount = Bytes.getInt(buffer, bufferIdx);
        bufferIdx += 4;
      }

      @Override
      public boolean hasNext() {
        if (bufferIdx >= writeIndex) {
          return false;
        }
        return true;
      }

      @Override
      public TimeSeriesId next() {
        if (seriesRead >= seriesCount) {
          // next metric;
          metricIdx++;
          seriesRead = 0;
          seriesCount = Bytes.getInt(buffer, bufferIdx);
          bufferIdx += 4;
        }

        long addr = Bytes.getLong(buffer, bufferIdx);
        bufferIdx += 8;

        int length = Bytes.getInt(buffer, bufferIdx);
        bufferIdx += 4;
        seriesRead++;
        requireDecodeBuffer(length);
        UnsafeHelper.unsafe.copyMemory(null, addr, decode_buffer, Unsafe.ARRAY_LONG_BASE_OFFSET, length);
        tags.clear();
        int start = 0;
        String key = null;
        for (int i = 0; i < length; i++) {
          if (decode_buffer[i] == 0) {
            // boundary
            String tag = new String(decode_buffer, start, i - start, Const.UTF8_CHARSET);
            if (key == null) {
              key = tag;
            } else {
              tags.put(key, tag);
              key = null;
            }
            start = i + 1;
          }
        }

        if (key != null) {
          String tag = new String(decode_buffer, start, length - start, Const.UTF8_CHARSET);
          tags.put(key, tag);
        }
        return this;
      }

      @Override
      public int compareTo(TimeSeriesStringId o) {
        throw new UnsupportedOperationException("TODO");
      }

      @Override
      public String alias() {
        return null;
      }

      @Override
      public String namespace() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String metric() {
        return strings[metricIdx];
      }

      @Override
      public Map<String, String> tags() {
        return tags;
      }

      @Override
      public String getTagValue(String key) {
        return tags.get(key);
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
        return Collections.emptySet();
      }

      @Override
      public long hits() {
        return 0;
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

      @Override
      public String toString() {
        return strings[metricIdx] + " " + tags;
      }
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public boolean isEmpty() {
      return size < 1;
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Iterator<TimeSeriesId> iterator() {
      return new It();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean add(TimeSeriesId e) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends TimeSeriesId> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * An iterator used for METRIC, TAG KEY and TAG VALUE queries.
   */
  class SingleStrings implements Collection<UniqueKeyPair<String, Long>> {

    class It implements Iterator<UniqueKeyPair<String, Long>> {
      UniqueKeyPair<String, Long> pair;
      int readIndex;

      It() {
        pair = new UniqueKeyPair<String, Long>(null, 0L);
      }

      @Override
      public boolean hasNext() {
        return readIndex < stringsIdx;
      }

      @Override
      public UniqueKeyPair<String, Long> next() {
        if (readIndex >= strings.length) {
          throw new RuntimeException("WTF? RI: " + readIndex + " and length: " + strings.length);
        }
        if ((readIndex * 4) >= writeIndex) {
          throw new RuntimeException("WTF? buffer: " + (readIndex * 4) + " over " + writeIndex + " of buffer " + buffer.length);
        }
        if ((readIndex * 4) >= buffer.length) {
          throw new RuntimeException("WTF? out of buffer: " + (readIndex * 4) + " over " + writeIndex + " of buffer " + buffer.length);
        }
        pair.setKey(strings[readIndex]);
        pair.setValue((long) Bytes.getInt(buffer, readIndex * 4));
        readIndex++;
        return pair;
      }

    }

    @Override
    public int size() {
      return stringsIdx;
    }

    @Override
    public boolean isEmpty() {
      return stringsIdx < 1;
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<UniqueKeyPair<String, Long>> iterator() {
      return new It();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(UniqueKeyPair<String, Long> e) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends UniqueKeyPair<String, Long>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * An iterator for TAG_KEY_AND_VALUE or BASIC queries.
   *
   * TODO - Right nowe we create the whole values list. We should be able to
   * make that one iterative as well.
   */
  class TagKeyAndValuesStrings implements Map<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> {

    class It implements Iterator<Entry<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>>>,
            Set<Entry<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>>>,
            Entry<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> {
      UniqueKeyPair<String, Long> key;
      List<UniqueKeyPair<String, Long>> values;

      int stringIndex;
      int bufferIdx;

      It() {
        key = new UniqueKeyPair<String, Long>(null, 0L);
        values = Lists.newArrayList();
      }

      @Override
      public boolean hasNext() {
        return stringIndex < stringsIdx;
      }

      @Override
      public Entry<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> next() {
        int keyCard = Bytes.getInt(buffer, bufferIdx);
        bufferIdx += 4;

        int count = Bytes.getInt(buffer, bufferIdx);
        bufferIdx += 4;

        key.setKey(strings[stringIndex++]);
        key.setValue((long) keyCard);

        values.clear();
        for (int i = 0; i < count; i++) {
          int valueCard = Bytes.getInt(buffer, bufferIdx);
          bufferIdx += 4;
          values.add(new UniqueKeyPair<String, Long>(strings[stringIndex++], (long) valueCard));
        }
        return this;
      }

      @Override
      public UniqueKeyPair<String, Long> getKey() {
        return key;
      }

      @Override
      public Collection<UniqueKeyPair<String, Long>> getValue() {
        return values;
      }

      @Override
      public Collection<UniqueKeyPair<String, Long>> setValue(Collection<UniqueKeyPair<String, Long>> value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
        return tagPairs;
      }

      @Override
      public boolean isEmpty() {
        return size == 0;
      }

      @Override
      public boolean contains(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterator<Entry<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>>> iterator() {
        return this;
      }

      @Override
      public Object[] toArray() {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean add(Entry<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> e) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(
              Collection<? extends Entry<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>>> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String toString() {
        return new StringBuilder()
                .append("key=[")
                .append(key)
                .append("], values=")
                .append(values)
                .toString();
      }
    }

    @Override
    public int size() {
      return tagPairs;
    }

    @Override
    public boolean isEmpty() {
      return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<UniqueKeyPair<String, Long>> get(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<UniqueKeyPair<String, Long>> put(UniqueKeyPair<String, Long> key,
                                                       Collection<UniqueKeyPair<String, Long>> value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<UniqueKeyPair<String, Long>> remove(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(
            Map<? extends UniqueKeyPair<String, Long>, ? extends Collection<UniqueKeyPair<String, Long>>> m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<UniqueKeyPair<String, Long>> keySet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Collection<UniqueKeyPair<String, Long>>> values() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>>> entrySet() {
      return new It();
    }

  }
}
