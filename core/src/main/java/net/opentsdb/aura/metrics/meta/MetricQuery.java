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

import java.nio.charset.StandardCharsets;

public class MetricQuery {
  private Query query;
  private byte[] metric;
  private String metricString;

  protected MetricQuery(QueryBuilder builder, String metric) {
    this(builder.build(), metric);
  }

  protected MetricQuery(Query query, String metric) {
    this.query = query;
    this.metric = metric.getBytes(StandardCharsets.UTF_8);
    metricString = metric;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Query getQuery() {
    return query;
  }

  public byte[] getMetric() {
    return metric;
  }

  public String getMetricString() {
    return metricString;
  }

  public static class Builder {
    private QueryBuilder queryBuilder;
    private String metric;
    private Query query;

    public Builder withBuilder(QueryBuilder queryBuilder) {
      this.queryBuilder = queryBuilder;
      return this;
    }

    public Builder forMetric(String metric) {
      this.metric = metric;
      return this;
    }

    public Builder withQuery(Query query) {
      this.query = query;
      return this;
    }

    public MetricQuery build() {
      if (queryBuilder != null) {
        return new MetricQuery(queryBuilder, metric);
      } else {
        return new MetricQuery(query, metric);
      }
    }
  }
}
