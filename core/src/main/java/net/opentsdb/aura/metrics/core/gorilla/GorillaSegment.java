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

import net.opentsdb.aura.metrics.core.LazyStatsCollector;
import net.opentsdb.aura.metrics.core.Segment;

public interface GorillaSegment extends Segment, LazyStatsCollector {

  int getSegmentTime();

  void setNumDataPoints(short numDataPoints);

  short getNumDataPoints();

  void setLastTimestamp(int lastTimestamp);

  int getLastTimestamp();

  void setLastValue(long value);

  long getLastValue();

  void setLastTimestampDelta(short lastTimestampDelta);

  short getLastTimestampDelta();

  void setLastValueLeadingZeros(byte lastLeadingZero);

  byte getLastValueLeadingZeros();

  void setLastValueTrailingZeros(byte lastTrailingZero);

  byte getLastValueTrailingZeros();

  long readData(final int bitsToRead);

  void writeData(long value, final int bitsToWrite);

  void updateHeader();

  void resetCursor();

  void reset();

  /**
   * Determines how many bytes are needed to serialize the current segment. If
   * the segment doesn't have any data (and that shouldn't happen as we should
   * not have created a segment if we don't have data to write).
   * @return A positive value reflecting the number of bytes to serialize.
   */
  int serializationLength();

  /**
   * Serializes the segment into the given byte buffer. For gorilla the serialized
   * data is as follows:
   * <ul>
   *     <li><b>1b</b> - Encoding type</li>
   *     <li><b>1 to 2b</b> - The number of data points. The MSB is a flag that
   *     when set, means there are 2 bytes for the length and if not set the
   *     length is only one byte. Make sure to mask this bit on reading.</li>
   *     <li><b>nb</b> - The gorilla encoded data. Note that Aura uses a long
   *     array and some bits/bytes from the final long may not be used. Those
   *     are dropped in this serialization and must be padded on read.</li>
   * </ul>
   * <b>NOTE</b> - The serialization does not include the data length or
   * references to segment timestamp or last time, leading or trailing zeros,
   * etc. The length and segment time must be handled by the flusher.
   *
   * @param buffer The non-null buffer to write to.
   * @param offset The starting offset to write to.
   * @param len The length of data we will be serializing.
   *            From {@link #serializationLength()}
   * @param lossy Whether or not this came from a lossy or lossless encoder.
   */
  void serialize(byte[] buffer, int offset, int len, boolean lossy);
}
