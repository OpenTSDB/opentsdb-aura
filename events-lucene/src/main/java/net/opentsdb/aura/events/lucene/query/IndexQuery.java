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

import org.apache.lucene.search.Query;

public class IndexQuery {

  private String index;
  private Query query;

  private int size;
  private int from;

  public IndexQuery(String index, Query query, int from, int size) {
    this.index = index;
    this.query = query;
    this.size = size;
    this.from = from;
  }

  public String getIndex() {
    return index;
  }

  public Query getQuery() {
    return query;
  }

  public int getSize() {
    return size;
  }

  public int getFrom() {
    return from;
  }

  @Override
  public String toString() {
    return "IndexQuery{" +
        "index='" + index + '\'' +
        ", query=" + query +
        ", size=" + size +
        ", from=" + from +
        '}';
  }
}