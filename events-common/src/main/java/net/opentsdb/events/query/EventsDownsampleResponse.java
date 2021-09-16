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

package net.opentsdb.events.query;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class EventsDownsampleResponse implements TimeSeries {

  private Set<IndexGroupResponse> groupResponse;

  public EventsDownsampleResponse(Set<IndexGroupResponse> groupResponse) {
    this.groupResponse = groupResponse;
  }

  public Set<IndexGroupResponse> getGroupResponse() {
    return groupResponse;
  }

  @Override
  public String toString() {
    return "EventsGroupResponse{" +
        "groupResponse=" + groupResponse +
        '}';
  }

  @Override
  public TimeSeriesId id() {
    return BaseTimeSeriesStringId.newBuilder().
        setMetric("count")
        .build();
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> typeToken) {
    if (typeToken.equals(NumericType.TYPE)) {
      TypedTimeSeriesIterator<? extends TimeSeriesDataType> localIterator = new LocalIterator();
      return Optional.of(localIterator);
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    TypedTimeSeriesIterator<? extends TimeSeriesDataType> localIterator = new LocalIterator();
    List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> list = new ArrayList<>();
    list.add(localIterator);
    return list;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return Lists.newArrayList(NumericType.TYPE);
  }

  @Override
  public void close() {

  }

  class LocalIterator implements TypedTimeSeriesIterator<NumericType> {

    MutableNumericValue numericValue = new MutableNumericValue();
    Iterator it = groupResponse.iterator();

    @Override
    public TypeToken<NumericType> getType() {
      return NumericType.TYPE;
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public TimeSeriesValue<NumericType> next() {
      IndexGroupResponse next = (IndexGroupResponse) it.next();
      long ts = Long.valueOf(next.getGroups().getValue());
      long count = next.getHits();
      SecondTimeStamp secondTimeStamp = new SecondTimeStamp(ts);
      numericValue.resetTimestamp(secondTimeStamp);
      numericValue.resetValue(count);
      return numericValue;
    }

    @Override
    public void close() throws IOException {

    }
  }

}


