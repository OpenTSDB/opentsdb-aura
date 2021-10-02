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

package net.opentsdb.aura.metrics;

import com.aerospike.client.cluster.Partition;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Hack to avoid creating millions of these at write or read time. Since the
 * {@link Partition} class is final and the fields final, and we can't consistently
 * use reflection we'll go the Unsafe route.
 */
public class ReusablePartition {
  static final Unsafe unsafe;

  static {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      unsafe = (Unsafe) f.get(null);
    } catch (NoSuchFieldException|IllegalAccessException  e) {
      throw new RuntimeException("Failed to get unsafe instance, are you " +
              "running Oracle JDK?", e);
    }
  }

  private final Partition partition;
  private final long offset;

  public ReusablePartition(final String namespace) {
//    partition = new Partition(namespace, 0);
    partition = null;
    try {
      final Field pid = Partition.class.getDeclaredField("partitionId");
      offset = unsafe.objectFieldOffset(pid);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("Failed to reflect into partition.", e);
    }
  }

  public void update(final int pid) {
    unsafe.putInt(partition, offset, pid);
  }

  public Partition get() {
    return partition;
  }
}
