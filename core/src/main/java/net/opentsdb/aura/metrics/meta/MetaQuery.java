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
import net.opentsdb.aura.metrics.meta.MetaQueryBuilder.MetaQueryType;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaQuery extends Query {

  AtomicInteger sizeCounter;
  int sizeLimit = 1024; // TEMP
  private final int tagCount;
  private final boolean exactMatch;
  protected Set<String> metrics;
  protected Map<String, Set<String>> tags;
  private boolean hasMetricFilter;
  private boolean hasTagOrAnyFilter;
  protected boolean scanMetrics;
  protected boolean scanTagKeys;
  protected String metaAggregationField;
  protected AnyFilter anyFilter;
  protected AnyFilter notAnyFilter;
  private Filter filter;
  private MetaQueryType metaQueryType; // may be null.

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  MetaQuery(final MetaQueryBuilder builder) {
    super(builder);
    //tagCount = builder.tagCount;
    exactMatch = builder.exactMatch;
    filter = builder.nestedFilter;
    metrics = builder.metrics;
    tags = builder.tags;
    metaQueryType = builder.metaQueryType;
    hasMetricFilter = builder.hasMetricFilter;
    hasTagOrAnyFilter = builder.hasTagOrAnyFilter;
    scanMetrics = builder.scanMetrics;
    scanTagKeys = builder.scanTagKeys;
    metaAggregationField = builder.metaAggregationField;
    if (builder.anyFilterBuilder != null) {
      anyFilter = builder.anyFilterBuilder.build();
    }
    if (builder.notAnyFilterBuilder != null) {
      notAnyFilter = builder.notAnyFilterBuilder.build();
    }
    sizeCounter = new AtomicInteger();
    tagCount = tags == null || tags.isEmpty()? builder.tagCount : tags.size();
  }

  public int getTagCount() {
    return tagCount;
  }

  public boolean isExactMatch() {
    return exactMatch;
  }
  
  public boolean hasMetricFilter() {
    return hasMetricFilter;
  }
  
  public boolean hasTagOrAnyFilter() {
    return hasTagOrAnyFilter;
  }
  
  public boolean scanMetrics() {
    return scanMetrics;
  }
  
  public boolean scanTagKeys() {
    return scanTagKeys;
  }
  
  public String metaAggregationField() {
    return metaAggregationField;
  }
  
  public AnyFilter anyFilter() {
    return anyFilter;
  }
  
  public AnyFilter notAnyFilter() {
    return notAnyFilter;
  }
  
  public Set<String> metrics() {
    return metrics;
  }
  
  public Map<String, Set<String>> tags() {
    return tags;
  }

  public Filter getFilter() {
    return filter;
  }
  
  public MetaQueryType metaQueryType() {
    return metaQueryType;
  }

  public int sizeRemaining() {
    int remaining = sizeLimit - sizeCounter.get();
    return remaining > 0 ? remaining : 0;
  }

  public boolean canAdd() {
    int size = sizeCounter.getAndIncrement();
    if (size > sizeLimit) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
    try {
      return new StringBuilder()
          .append("{tagCount=")
          .append(tagCount)
          .append("{metrics=")
          .append(metrics)
          .append(", tags=")
          .append(tags)
          .append(", hasMetricOrAnyFilter=")
          .append(hasMetricFilter)
          .append(", hasTagOrAnyFilter=")
          .append(hasTagOrAnyFilter)
          .append(", scanMetrics")
          .append(scanMetrics)
          .append(", scanTagKeys")
          .append(scanTagKeys)
          .append(", metaQueryType=")
          .append(metaQueryType)
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