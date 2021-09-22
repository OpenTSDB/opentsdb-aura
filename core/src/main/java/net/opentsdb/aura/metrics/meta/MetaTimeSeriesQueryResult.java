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

import org.roaringbitmap.RoaringBitmap;

public interface MetaTimeSeriesQueryResult {

  /**
   * @return An exception if one was thrown, null if not.
   */
  Throwable getException();

  /**
   * Fetches the string from the hash table.
   *
   * @param hash The hash to fetch.
   * @return The string or null if not found (shouldn't happen)
   */
  String getStringForHash(final long hash);

  /**
   * The total number of groups in the result. For raw queries we'll have one
   * group per time series.
   *
   * @return The number of groups in the result. 0 if no results for the query.
   */
  int numGroups();

  /**
   * @return The number of unique time series hashes across all groups.
   */
  int totalHashes();

  /**
   * @return The total number of results expected from a remote source.
   */
  int totalResults();

  /**
   * Returns the group for the given index based on {@link #numGroups()}.
   * <b>NOTE:</b> This call must be thread safe.
   *
   * @param index An index less than {@link #numGroups()}.
   * @return A group result instance or null if the index was out of bounds.
   */
  GroupResult getGroup(int index);

  /**
   * A grouped (or possibly raw) query result with at least 1 time series in the
   * group.
   */
  interface GroupResult {
    /**
     * @return A hash uniquely identifying the group (used for merging from the
     * meta shards). For raw queries this should be the has of the time series.
     */
    long id();

    /**
     * @return The number of unique time series hashes in this group.
     */
    int numHashes();

    /**
     * Returns the time series hash at the given index less than {@link #numHashes()}.
     * <b>NOTE:</b> This call must be thread safe. (shared across group by workers)
     *
     * @param index An index from 0 to {@link #numHashes()} - 1.
     * @return The time series hash at the given index.
     */
    long getHash(int index);

    /**
     * Returns the epoch bitmap for a timeseries at the given index.
     * @param index
     * @return
     */
    RoaringBitmap getBitMap(int index);

    /**
     * @return The tag hash instance used for creating an ID. For a group-all we should
     * return a default instance with a size of 0.
     */
    TagHashes tagHashes();

    /**
     * Can't be a Java Iterable as we're returning a primitive so we avoid autoboxing
     * here.
     */
    interface TagHashes {
      /**
       * @return The number of string identifiers. If raw series, it's the
       * sum of tag keys and tag values. For grouped results it's the sum of
       * tag keys. May be zero for group-all queries.
       */
      int size();

      /**
       * @return An iterator over the values or keys and values (alternately)
       */
      long next();
    }

    interface TimestampMatcher {
      boolean match(int ts);
    }
  }
}