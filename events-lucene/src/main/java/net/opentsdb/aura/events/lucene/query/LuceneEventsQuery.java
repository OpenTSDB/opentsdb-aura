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

package net.opentsdb.aura.events.lucene.query;

import net.opentsdb.events.query.EventsQuery;

import java.util.List;

public class LuceneEventsQuery implements EventsQuery {

  private List<IndexQuery> luceneQueryPerIndex;
  private String groupBy;
  private boolean fetchLast;

  public LuceneEventsQuery(List<IndexQuery> luceneQueryPerIndex, String groupBy, boolean fetchLast) {
    this.luceneQueryPerIndex = luceneQueryPerIndex;
    this.groupBy = groupBy;
    this.fetchLast = fetchLast;
  }

  public List<IndexQuery> getLuceneQueryPerIndex() {
    return luceneQueryPerIndex;
  }

  public String getGroupBy() {
    return groupBy;
  }

  public boolean fetchLast() {
    return fetchLast;
  }


  @Override
  public String toString() {
    return "LuceneEventsQuery{" +
        "luceneQueryPerIndex=" + luceneQueryPerIndex +
        ", groupBy='" + groupBy + '\'' +
        ", fetchLast='" + fetchLast + '\'' +
        '}';
  }
}
