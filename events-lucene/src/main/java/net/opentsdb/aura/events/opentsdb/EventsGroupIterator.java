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

package net.opentsdb.aura.events.opentsdb;

import com.google.common.reflect.TypeToken;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.event.EventGroupType;
import net.opentsdb.data.types.event.EventsGroupValue;

import java.io.IOException;

public class EventsGroupIterator extends EventsGroupValue implements TypedTimeSeriesIterator {

  boolean has_next = true;

  public EventsGroupIterator(Builder builder) {
    super(builder);
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return EventGroupType.TYPE;
  }

  @Override
  public boolean hasNext() {
    boolean had = has_next;
    has_next = false;
    return had;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    return this;
  }

  @Override
  public void close() throws IOException {

  }
}
