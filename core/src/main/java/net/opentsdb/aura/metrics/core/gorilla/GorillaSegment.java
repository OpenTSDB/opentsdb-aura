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

package net.opentsdb.aura.metrics.core.gorilla;

import net.opentsdb.aura.metrics.core.Segment;
import net.opentsdb.aura.metrics.core.data.BitMap;

public interface GorillaSegment extends Segment, BitMap {

  void updateHeader();

  boolean moveToHead();

  boolean moveToTail();

  default int headerLengthBytes() {
    throw new UnsupportedOperationException();
  }

  default int dataLengthBytes() {
    throw new UnsupportedOperationException();
  }

  default void serialize(int srcOffset, byte[] dest, int destOffset, int length) {
    throw new UnsupportedOperationException();
  }
}
