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

import net.opentsdb.aura.metrics.core.ShardConfig;
import net.opentsdb.aura.metrics.meta.MetaDataStoreFactory;
import net.opentsdb.aura.metrics.meta.NewDocStore;
import net.opentsdb.hashing.HashFunction;

public class NewDocStoreFactory implements MetaDataStoreFactory<NewDocStore> {
  private ShardConfig config;
  private HashFunction hashFunction;

  public NewDocStoreFactory(final ShardConfig config, HashFunction hashFunction) {
    this.config = config;
    this.hashFunction = hashFunction;
  }

  @Override
  public NewDocStore create() {
    return
            new NewDocStore(
                    config.metaStoreCapacity, config.metaPurgeBatchSize, null, hashFunction, new MetaSearchResults(), config.metaQueryEnabled);
  }
}
