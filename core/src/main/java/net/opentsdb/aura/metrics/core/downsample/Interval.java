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
  private final int seconds;

  Interval(final byte id, final int seconds) {
    this.id = id;
    this.seconds = seconds;
  }

  public byte getId() {
    return id;
  }

  public int getSeconds() {
    return seconds;
  }

  public short getCount(SegmentWidth segmentWidth) {
    return (short) (segmentWidth.getWidth() / seconds);
  }

  public static Interval getById(byte id) {
    return values()[id];
  }

  public static Interval getBySeconds(final int seconds) {
    if (seconds == _5_SEC.seconds) {
      return _5_SEC;
    } else if (seconds == _10_SEC.seconds) {
      return _10_SEC;
    } else if (seconds == _15_SEC.seconds) {
      return _15_SEC;
    } else if (seconds == _30_SEC.seconds) {
      return _30_SEC;
    } else if (seconds == _1_MIN.seconds) {
      return _1_MIN;
    } else if (seconds == _5_MIN.seconds) {
      return _5_MIN;
    } else if (seconds == _10_MIN.seconds) {
      return _10_MIN;
    } else if (seconds == _15_MIN.seconds) {
      return _15_MIN;
    } else if (seconds == _30_MIN.seconds) {
      return _30_MIN;
    } else if (seconds == _1_HR.seconds) {
      return _1_HR;
    } else if (seconds == _1_DAY.seconds) {
      return _1_DAY;
    } else {
      throw new IllegalArgumentException("No interval found for " + seconds + " seconds");
    }
  }

  public static Interval getByMinutes(final int minutes) {
    try {
      return getBySeconds((int) MINUTES.toSeconds(minutes));
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("No interval found for " + minutes + " minutes");
    }
  }

  public static Interval getByHours(final int hours) {
    try {
      return getBySeconds((int) HOURS.toSeconds(hours));
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("No interval found for " + hours + " hours");
    }
  }

  public static Interval getByDays(final int days) {
    try {
      return getBySeconds((int) DAYS.toSeconds(days));
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("No interval found for " + days + " days");
    }
  }
}
