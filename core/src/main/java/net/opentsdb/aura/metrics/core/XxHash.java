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

package net.opentsdb.aura.metrics.core;

import net.opentsdb.hashing.HashFunction;
import net.opentsdb.utils.XXHash;

public class XxHash implements HashFunction {
  @Override
  public long hash(byte[] bytes) {
    return XXHash.hash(bytes);
  }

  @Override
  public long hash(byte[] bytes, int offset, int length) {
    return XXHash.hash(bytes, offset, length);
  }

  @Override
  public long update(long previousHash, byte[] bytes) {
    return XXHash.updateHash(previousHash, bytes);
  }

  @Override
  public long update(long previousHash, byte[] bytes, int offset, int len) {
    return XXHash.updateHash(previousHash, bytes, offset, len);
  }

  @Override
  public long update(long hashA, long hashB) {
    return XXHash.combineHashes(hashA, hashB);
  }
}
