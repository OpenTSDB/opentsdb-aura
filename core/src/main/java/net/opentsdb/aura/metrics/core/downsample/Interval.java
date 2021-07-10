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
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public enum Interval {
  _5_SEC((byte) 0, (int) SECONDS.toSeconds(5)),
  _10_SEC((byte) 1, (int) SECONDS.toSeconds(10)),
  _15_SEC((byte) 2, (int) SECONDS.toSeconds(15)),
  _30_SEC((byte) 3, (int) SECONDS.toSeconds(30)),
  _1_MIN((byte) 4, (int) MINUTES.toSeconds(1)),
  _5_MIN((byte) 5, (int) MINUTES.toSeconds(5)),
  _10_MIN((byte) 6, (int) MINUTES.toSeconds(10)),
  _15_MIN((byte) 7, (int) MINUTES.toSeconds(15)),
  _30_MIN((byte) 8, (int) MINUTES.toSeconds(30)),
  _1_HR((byte) 9, (int) HOURS.toSeconds(1)),
  _1_DAY((byte) 10, (int) DAYS.toSeconds(1));

  private final byte id;
  private final int width;

  Interval(final byte id, final int width) {
    this.id = id;
    this.width = width;
  }

  public byte getId() {
    return id;
  }

  public int getWidth() {
    return width;
  }

  public short getCount(SegmentWidth segmentWidth) {
    return (short) (segmentWidth.getWidth() / width);
  }

  public Interval getById(byte id) {
    return values()[id];
  }
}
