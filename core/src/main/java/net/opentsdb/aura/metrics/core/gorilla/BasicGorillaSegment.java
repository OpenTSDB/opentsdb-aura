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

public interface BasicGorillaSegment extends GorillaSegment, LazyStatsCollector {

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

}
