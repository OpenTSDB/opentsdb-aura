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

import gnu.trove.iterator.TLongIntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MergedMetaTimeSeriesQueryResult implements MetaResultWithDictionary {
  private static final Logger logger = LoggerFactory.getLogger(MergedMetaTimeSeriesQueryResult.class);

  // TODO: Converting this into a map will make look ups faster.
  // TODO: Atleast we can maintain another array with the hashes to find the index faster.
  private DefaultMetaTimeSeriesQueryResult.DefaultGroupResult[] groupResults =
          new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult[32];
  private Throwable exception;
  private DefaultMetaTimeSeriesQueryResult.DefaultDictionary dictionary =
          new DefaultMetaTimeSeriesQueryResult.DefaultDictionary();
  private int groupCount;
  private int hashCount;

  public void add(MetaResultWithDictionary result) {
    if (exception != null) {
      return;
    }

    if (result.getException() != null) {
      exception = result.getException();
      return;
    }

    NextGroup:
    for (int i = 0; i < result.numGroups(); i++) {
      DefaultMetaTimeSeriesQueryResult.DefaultGroupResult remoteGroup =
              (DefaultMetaTimeSeriesQueryResult.DefaultGroupResult) result.getGroup(i);
      for (int x = 0; x < groupCount; x++) {
        if (groupResults[x].id() == remoteGroup.id()) {
          // matched on the group
          DefaultMetaTimeSeriesQueryResult.DefaultGroupResult localGR = groupResults[x];
          int initGroupSize = localGR.numHashes();
          for (int idx = 0; idx < remoteGroup.numHashes(); idx++) {
            long hash = remoteGroup.getHash(idx);
            int f = Arrays.binarySearch(localGR.hashes, 0, localGR.numHashes(), hash);
            if (f < 0) {
              localGR.addHash(hash);
              localGR.addBitMap(hash, remoteGroup.getBitMap(idx));
              ++hashCount;
            }
          }

          // re-sort if we added anything.
          if (localGR.numHashes() != initGroupSize) {
            Arrays.sort(localGR.hashes, 0, localGR.numHashes());
          }
          continue NextGroup;
        }
      }

      // no match
      if (groupCount + 1 >= groupResults.length) {
        DefaultMetaTimeSeriesQueryResult.DefaultGroupResult[] temp =
                new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult[groupResults.length * 2];
        System.arraycopy(groupResults, 0, temp, 0, groupCount);
        groupResults = temp;
      }
      Arrays.sort(remoteGroup.hashes, 0, remoteGroup.numHashes());
      groupResults[groupCount++] = remoteGroup;
      hashCount += remoteGroup.numHashes();
    }

    // Dictionary
    result.getDictionary().mergeInto(dictionary);
  }

  @Override
  public Throwable getException() {
    return exception;
  }

  @Override
  public String getStringForHash(long hash) {
    byte[] bytes = dictionary.get(hash);
    if (bytes == null) {
      return null;
    }
    return new String(bytes);
  }

  @Override
  public int numGroups() {
    return groupCount;
  }

  @Override
  public int totalHashes() {
    return hashCount;
  }

  @Override
  public int totalResults() {
    return 1;
  }

  @Override
  public GroupResult getGroup(int index) {
    return groupResults[index];
  }

  @Override
  public Dictionary getDictionary() {
    return dictionary;
  }
}