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

package net.opentsdb.aura.metrics.storage;

import com.fasterxml.jackson.core.JsonGenerator;
import net.opentsdb.core.Const;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.JSON;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MystQueryBuilder {

  private MystQueryBuilder() {

  }

  public static String build(String namespace, QueryFilter filter, String metric, int start, int end, String[] groupByTags) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {

      MetricLiteralFilter metricFilter = MetricLiteralFilter.newBuilder()
              .setMetric(metric)
              .build();
      ChainFilter.Builder chainBuilder = ChainFilter.newBuilder()
              .setOp(ChainFilter.FilterOp.AND)
              .addFilter(metricFilter);

      if (filter instanceof ExplicitTagsFilter) {
        chainBuilder.addFilter(((ExplicitTagsFilter) filter).getFilter());
        filter = ExplicitTagsFilter.newBuilder()
                .setFilter(chainBuilder.build())
                .build();
      } else if (filter != null) {
        filter = chainBuilder.addFilter(filter).build();
      } else {
        filter = chainBuilder.build();
      }

      JsonGenerator json = JSON.getFactory().createGenerator(baos);
      json.writeStartObject();

      json.writeNumberField("from", 0);
      json.writeNumberField("to", 1);
      json.writeNumberField("start", start);
      json.writeNumberField("end", end);
      json.writeStringField("order", "ASCENDING");
      json.writeStringField("type", "TIMESERIES");

      json.writeArrayFieldStart("group");
      if (groupByTags != null) {
        for (int i = 0; i < groupByTags.length; i++) {
          json.writeString(groupByTags[i]);
        }
      }
      json.writeEndArray();

      json.writeStringField("namespace", namespace);

      json.writeRaw(",\"query\":");
      json.writeRaw(JSON.serializeToString(filter));

      json.writeEndObject();
      json.close();
      return new String(baos.toByteArray(), Const.UTF8_CHARSET);
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
  }

}