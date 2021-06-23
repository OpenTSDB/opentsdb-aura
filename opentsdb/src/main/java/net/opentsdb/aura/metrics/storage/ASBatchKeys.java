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

import net.opentsdb.aura.metrics.LTSAerospike;

public class ASBatchKeys {
  public static final int DEFAULT_BATCH_LIMIT = 1024;

  protected byte[][] keys;
  protected long[] groupIds;
  protected long[] hashes;
  protected int[] groupIdx;
  protected int keyIdx = 0;

  ASBatchKeys() {
    keys = new byte[DEFAULT_BATCH_LIMIT][];
    for (int i = 0; i < DEFAULT_BATCH_LIMIT; i++) {
      keys[i] = new byte[LTSAerospike.KEY_LENGTH];
    }
    groupIds = new long[DEFAULT_BATCH_LIMIT];
    hashes = new long[DEFAULT_BATCH_LIMIT];
    groupIdx = new int[DEFAULT_BATCH_LIMIT];
  }

  void grow(final int newLimit) {
    if (newLimit <= keys.length) {
      // no-op
      return;
    }

    byte[][] tempKeys = new byte[newLimit][];
    System.arraycopy(keys, 0, tempKeys, 0, keyIdx);
    keys = tempKeys;
    for (int i = keyIdx; i < keys.length; i++) {
      keys[i] = new byte[LTSAerospike.KEY_LENGTH];
    }

    long[] tempLong = new long[newLimit];
    System.arraycopy(groupIds, 0, tempLong, 0, keyIdx);
    groupIds = tempLong;

    tempLong = new long[newLimit];
    System.arraycopy(hashes, 0, tempLong, 0, keyIdx);
    hashes = tempLong;

    int[] tempInt = new int[newLimit];
    System.arraycopy(groupIdx, 0, tempInt, 0, keyIdx);
    groupIdx = tempInt;
  }

}