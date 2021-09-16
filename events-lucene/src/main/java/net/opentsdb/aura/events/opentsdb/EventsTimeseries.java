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

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.event.EventGroupType;
import net.opentsdb.data.types.event.EventType;
import net.opentsdb.data.types.event.EventsValue;
import net.opentsdb.data.types.event.EventsValue.Builder;
import net.opentsdb.events.query.IndexGroupResponse;
import net.opentsdb.events.view.Event;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public class EventsTimeseries implements TimeSeries {

  /**
   * The parsed ID.
   */
  private final TimeSeriesStringId id;
  /**
   * The parsed out types.
   */
  private final List<TypeToken<? extends TimeSeriesDataType>> types;
  private final Event event;
  TypedTimeSeriesIterator iterator;


  EventsTimeseries(final Event event, final long hits) {

    try {
      this.event = event;
      BaseTimeSeriesStringId.Builder id_builder =
          BaseTimeSeriesStringId.newBuilder().setMetric("ignored");
      if (event.getDimensions() != null) {
        for (Entry<String, String> entry : event.getDimensions().entrySet()) {
          id_builder.addTags(entry.getKey(), entry.getValue());
        }
      }
      id_builder.setHits(hits);
      id = id_builder.build();
      types = Lists.newArrayList(EventType.TYPE);
      Builder builder = EventsIterator.newBuilder();
      builder.setEventId(event.getEventId()).
          setNamespace(event.getNamespace()).
          setSource(event.getSource()).
          setTimestamp(event.getTimestamp()).
          setEndTimestamp(event.getEndTimestamp()).
          setPriority(event.getPriority()).
          setUserId(event.getUserId()).
          setTitle(event.getTitle()).
          setMessage(event.getMessage()).
          setOngoing(event.isOngoing()).
          setParentIds(event.getParentIds()).
          setChildIds(event.getChildIds()).
          setAdditionalProps(event.getAdditionalProps());

      iterator = new EventsIterator(builder);

    } catch (Throwable throwable) {
      throw throwable;
    }
  }

  EventsTimeseries(final IndexGroupResponse groupResponse) {

    try {
      BaseTimeSeriesStringId.Builder id_builder =
          BaseTimeSeriesStringId.newBuilder().setMetric("ignored");

      id_builder.setHits(groupResponse.getHits());
      types = Lists.newArrayList(EventGroupType.TYPE);

      EventsGroupIterator.Builder builder = EventsGroupIterator.newBuilder();
      Map<String, String> group = new HashMap<>();
      group.put(groupResponse.getGroups().getKey(), groupResponse.getGroups().getValue());
      builder.setGroup(group);


      this.event = groupResponse.getEvent();
      if (event != null) {
        if (event.getDimensions() != null) {
          for (Entry<String, String> entry : event.getDimensions().entrySet()) {
            id_builder.addTags(entry.getKey(), entry.getValue());
          }
        }
        Builder eventsValueBuilder = EventsValue.newBuilder();
        eventsValueBuilder.setEventId(event.getEventId()).
            setNamespace(event.getNamespace()).
            setSource(event.getSource()).
            setTimestamp(event.getTimestamp()).
            setEndTimestamp(event.getEndTimestamp()).
            setPriority(event.getPriority()).
            setUserId(event.getUserId()).
            setTitle(event.getTitle()).
            setMessage(event.getMessage()).
            setOngoing(event.isOngoing()).
            setParentIds(event.getParentIds()).
            setChildIds(event.getChildIds()).
            setAdditionalProps(event.getAdditionalProps());

        builder.setEvent(eventsValueBuilder.build());
      }
      id = id_builder.build();
      iterator = new EventsGroupIterator(builder);

    } catch (Throwable throwable) {
      throw throwable;
    }
  }

  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (type.equals(EventType.TYPE)) {
      return Optional.of(iterator);
    } else if (type.equals(EventGroupType.TYPE)) {
      return Optional.of(iterator);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = Lists.newArrayList();
    its.add(iterator);
    return its;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return types;
  }

  @Override
  public void close() {
  }
}
