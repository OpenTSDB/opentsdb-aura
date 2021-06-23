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

package net.opentsdb.aura.metrics.meta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Set;

public class Query {

  private final int tagCount;
  private final boolean exactMatch;
  protected Map<String, Set<String>> tags;
  private Filter filter;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected Query(final QueryBuilder builder) {
    exactMatch = builder.exactMatch;
    filter = builder.nestedFilter;
    tags = builder.tags;
    tagCount = tags == null || tags.isEmpty()? builder.tagCount : tags.size();
  }

  public int getTagCount() {
    return tagCount;
  }

  public boolean isExactMatch() {
    return exactMatch;
  }
  
  public Map<String, Set<String>> tags() {
    return tags;
  }

  public Filter getFilter() {
    return filter;
  }
    
  @Override
  public String toString() {
    try {
      return new StringBuilder()
          .append("{tagCount=")
          .append(tagCount)
          .append(", tags=")
          .append(tags)
          .append(", exactMatch=")
          .append(exactMatch)
          .append(", filter= ")
          .append(OBJECT_MAPPER.writeValueAsString(filter))
          .append("}")
          .toString();
    } catch (JsonProcessingException e) {
      return e.getMessage();
    }
  }
}