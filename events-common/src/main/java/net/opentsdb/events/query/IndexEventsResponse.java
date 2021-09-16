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

import net.opentsdb.events.view.Event;

import java.util.List;

public class IndexEventsResponse {

  private List<Event> events;
  private long hits;
  private String index;


  public IndexEventsResponse(List<Event> events, long hits, String index) {
    this.events = events;
    this.hits = hits;
    this.index = index;
  }

  public List<Event> getEvents() {
    return events;
  }

  public long getHits() {
    return hits;
  }

  public String getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return "IndexEventsResponse{" +
        "events=" + events +
        ", hits=" + hits +
        ", index=" + index +
        '}';
  }
}
