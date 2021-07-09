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

import net.opentsdb.aura.metrics.core.data.FixedSizedMemoryBlockStream;

public abstract class OffHeapSegment extends FixedSizedMemoryBlockStream implements Segment {

  protected static final int SEGMENT_TIME_BYTE_INDEX = 18;

  public OffHeapSegment(int blockSizeBytes) {
    super(blockSizeBytes);
  }

  @Override
  public long create(int segmentTime) {
    long address = allocate();
    header.setInt(SEGMENT_TIME_BYTE_INDEX, segmentTime);
    return address;
  }

  @Override
  public void open(long segmentAddress) {
    load(segmentAddress);
  }

  @Override
  public int getSegmentTime() {
    return header.getInt(SEGMENT_TIME_BYTE_INDEX);
  }

}
