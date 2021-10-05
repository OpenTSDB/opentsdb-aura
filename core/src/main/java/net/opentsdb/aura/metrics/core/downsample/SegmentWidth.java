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
  private final int seconds;

  SegmentWidth(final byte id, final int seconds) {
    this.id = id;
    this.seconds = seconds;
  }

  public byte getId() {
    return id;
  }

  public int getSeconds() {
    return seconds;
  }

  public static SegmentWidth getById(final byte id) {
    return values()[id];
  }

  public static SegmentWidth getBySeconds(final int seconds) {
    if (seconds == _1_HR.seconds) {
      return _1_HR;
    } else if (seconds == _2_HR.seconds) {
      return _2_HR;
    } else if (seconds == _6_HR.seconds) {
      return _6_HR;
    } else if (seconds == _24_HR.seconds) {
      return _24_HR;
    } else if (seconds == _1_WEEK.seconds) {
      return _1_WEEK;
    } else if (seconds == _6_WEEK.seconds) {
      return _6_WEEK;
    } else {
      throw new IllegalArgumentException("No Segment found for " + seconds + " seconds");
    }
  }

  public static SegmentWidth getByHours(final int hours) {
    try {
      return getBySeconds((int) HOURS.toSeconds(hours));
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("No Segment found for " + hours + " hours");
    }
  }

  public static SegmentWidth getByWeeks(final int weeks) {
    try {
      return getBySeconds((int) DAYS.toSeconds(weeks * 7));
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("No Segment found for " + weeks + " weeks");
    }
  }
}
