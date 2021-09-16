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

public class IndexGroupResponse implements Comparable<IndexGroupResponse> {

  private Group group;
  private long hits;
  private Event event;
  private String index;
  private boolean isDownsample;

  public IndexGroupResponse(String index, Group group, long hits, Event event, boolean isDownsample) {
    this.group = group;
    this.hits = hits;
    this.event = event;
    this.index = index;
    this.isDownsample = isDownsample;
  }

  public Group getGroups() {
    return group;
  }

  public long getHits() {
    return hits;
  }

  public void addHits(long hits) {
    this.hits = this.hits + hits;
  }

  public Event getEvent() {
    return event;
  }

  public String getIndex() {
    return index;
  }

  public void addEvent(Event event) {
   if (this.event.getTimestamp() < event.getTimestamp()) {
     this.event = event;
   }
  }


  @Override
  public String toString() {
    return "IndexGroupResponse{" +
        "group=" + group +
        ", hits=" + hits +
        ", event=" + event +
        '}';
  }

  @Override
  public int compareTo(IndexGroupResponse o) {

    if (isDownsample) {

      if (Long.valueOf(o.getGroups().getValue()) < Long.valueOf(this.getGroups().getValue())) {
        return 1;
      } else if ( Long.valueOf(o.getGroups().getValue()) > Long.valueOf(this.getGroups().getValue())) {
        return -1;
      } else {
        return 0;
      }
    } else {
      if (! o.getGroups().equals(this.getGroups())) {
        return o.getGroups().compareTo(this.getGroups());
      }
      if (! o.getIndex().equals(this.getIndex())) {
        return o.getIndex().compareTo(this.getIndex());
      }
      return ((Long) o.getHits()).compareTo(this.getHits());
    }
  }
  
}

