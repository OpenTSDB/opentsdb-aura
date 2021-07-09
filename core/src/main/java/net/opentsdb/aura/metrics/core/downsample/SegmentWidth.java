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

package net.opentsdb.aura.metrics.core.downsample;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;

public enum SegmentWidth {
  _1_HR((byte) 0, (int) HOURS.toSeconds(1)),
  _2_HR((byte) 1, (int) HOURS.toSeconds(2)),
  _6_HR((byte) 2, (int) HOURS.toSeconds(6)),
  _24_HR((byte) 3, (int) HOURS.toSeconds(24)),
  _1_WEEK((byte) 4, (int) DAYS.toSeconds(7)),
  _6_WEEK((byte) 5, (int) DAYS.toSeconds(42));

  private final byte id;
  private final int width;

  SegmentWidth(final byte id, final int width) {
    this.id = id;
    this.width = width;
  }

  public byte getId() {
    return id;
  }

  public int getWidth() {
    return width;
  }

  public SegmentWidth getById(final byte id) {
    return values()[id];
  }
}
