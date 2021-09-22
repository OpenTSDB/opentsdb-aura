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

import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import net.opentsdb.utils.XXHash;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class DefaultMetaTimeSeriesQueryResult implements MetaTimeSeriesQueryResult {
  private static final Logger logger = LoggerFactory.getLogger(DefaultMetaTimeSeriesQueryResult.class);

  private static final int DEFAULT_ARRAY_SIZE = 32;

  private MetaTimeSeriesQueryResult.GroupResult[] groupResults =
          new MetaTimeSeriesQueryResult.GroupResult[DEFAULT_ARRAY_SIZE];
  protected DefaultDictionary dictionary;

  private int groupCount;
  private int hashCount;
  private int totalResults;

  private Throwable exception;

  public void reset() {
    groupCount = 0;
    hashCount = 0;
    totalResults = 0;
  }

  public void addGroupResult(final MetaTimeSeriesQueryResult.GroupResult groupResult) {
    if (groupCount + 1 > groupResults.length) {
      MetaTimeSeriesQueryResult.GroupResult[] temp =
              new MetaTimeSeriesQueryResult.GroupResult[groupCount * 2];
      System.arraycopy(groupResults, 0, temp, 0, groupCount);
      groupResults = temp;
    }
    if (groupResults[groupCount] == null) {
      groupResults[groupCount] = groupResult;
    } else {

    }
    groupCount++;
    hashCount += groupResult.numHashes();
  }

  public void setDictionary(DefaultDictionary dictionary) {
    this.dictionary = dictionary;
  }

  public void setException(final Throwable exception) {
    this.exception = exception;
  }

  public void setTotalResults(final int totalResults) {
    this.totalResults = totalResults;
  }

  @Override
  public Throwable getException() {
    return exception;
  }

  @Override
  public String getStringForHash(final long hash) {
    return new String(dictionary.get(hash));
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
    return totalResults;
  }

  @Override
  public GroupResult getGroup(final int index) {
    return groupResults[index];
  }

  public static class DefaultGroupResult implements GroupResult {
    private long[] tagHashes = new long[DEFAULT_ARRAY_SIZE];
    public long[] hashes = new long[DEFAULT_ARRAY_SIZE];
    public RoaringBitmap[] bitmaps = new RoaringBitmap[DEFAULT_ARRAY_SIZE];

    private long id = 0;
    private int tagHashCount;
    private int hashCount;

    public void reset() {
      id = 0;
      tagHashCount = 0;
      hashCount = 0;
    }

    public void addTagHash(final long tagHash) {
      int oldLength = tagHashes.length;
      if (tagHashCount >= oldLength) {
        int newLength = (int) (oldLength * 1.5);
        long[] newTagHashes = new long[newLength];
        System.arraycopy(tagHashes, 0, newTagHashes, 0, oldLength);
        tagHashes = newTagHashes;
      }
      tagHashes[tagHashCount++] = tagHash;
    }

    public void addHash(final long hash) {
      int oldLength = hashes.length;
      if (hashCount >= oldLength) {
        int newLength = (int) (oldLength * 1.5);
        long[] newHashes = new long[newLength];
        System.arraycopy(hashes, 0, newHashes, 0, oldLength);
        hashes = newHashes;
        RoaringBitmap[] newBitmaps = new RoaringBitmap[newLength];
        System.arraycopy(bitmaps, 0, newBitmaps, 0, oldLength);
        bitmaps = newBitmaps;
      }
      hashes[hashCount++] = hash;
    }

    public void addBitMap(long hash, byte[] bitMap) throws IOException {
      //Will leave the hash as is, even though we are not using it.
      RoaringBitmap roaringBitmap = new RoaringBitmap();
      roaringBitmap.deserialize(ByteBuffer.wrap(bitMap));
      bitmaps[hashCount - 1] = roaringBitmap;
    }

    public void addBitMap(long hash, RoaringBitmap bitmap) {
      bitmaps[hashCount] = bitmap;
    }

    @Override
    public long id() {
      if (id == 0 && tagHashCount > 0) {
        long[] clone = Arrays.copyOfRange(tagHashes, 0, tagHashCount);
        Arrays.sort(clone);
        id = clone[0];
        for (int i = 1; i < clone.length; i++) {
          id = XXHash.combineHashes(id, clone[i]);
        }
      }
      return id;
    }

    @Override
    public int numHashes() {
      return hashCount;
    }

    @Override
    public long getHash(final int index) {
      return hashes[index];
    }

    @Override
    public RoaringBitmap getBitMap(int index) {
      return bitmaps[index];
    }


    @Override
    public TagHashes tagHashes() {
      return new TagHashes() {
        int i = 0;

        @Override
        public int size() {
          return tagHashCount;
        }

        @Override
        public long next() {
          if (i >= tagHashCount) {
            throw new IndexOutOfBoundsException(String.valueOf(i));
          }
          return tagHashes[i++];
        }
      };
    }
  }

  public interface Dictionary {
    void put(final long id, final byte[] value);

    byte[] get(final long id);

    int size();
  }

  public static class DefaultDictionary implements Dictionary {

    private int nextIndex = 0;
    protected TLongIntMap indexMap;
    protected byte[][] values;

    public DefaultDictionary() {
      this(DEFAULT_ARRAY_SIZE);
    }

    public DefaultDictionary(final int capacity) {
      this.indexMap = new TLongIntHashMap(capacity, 0.75f, Long.MIN_VALUE, Integer.MIN_VALUE);
      this.values = new byte[capacity][];
    }

    public void reset() {
      nextIndex = 0;
      indexMap.clear();
    }

    @Override
    public void put(final long id, final byte[] value) {
      int index = indexMap.get(id);
      if (indexMap.getNoEntryValue() == index) {
        indexMap.put(id, nextIndex);
        index = nextIndex++;
        int oldLength = values.length;
        if (index >= oldLength) {
          int newLength = (int) (oldLength * 1.5);
          byte[][] newValues = new byte[newLength][];
          System.arraycopy(values, 0, newValues, 0, oldLength);
          this.values = newValues;
        }
        values[index] = value;
      }
    }

    @Override
    public byte[] get(final long id) {
      int index = indexMap.get(id);
      if (indexMap.getNoEntryValue() == index) {
        return null;
      }
      return values[index];
    }

    @Override
    public int size() {
      return nextIndex;
    }

  }

}