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

import net.opentsdb.aura.events.lucene.IndexManager;
import net.opentsdb.aura.events.lucene.query.LuceneEventsQuery;
import net.opentsdb.aura.events.lucene.query.LuceneQueryBuilder;
import net.opentsdb.aura.events.lucene.query.QueryRunner;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.events.query.EventsDownsampleResponse;
import net.opentsdb.events.query.EventsGroupResponse;
import net.opentsdb.events.query.EventsResponse;
import net.opentsdb.events.query.IndexEventsResponse;
import net.opentsdb.events.query.IndexGroupResponse;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

public class EventsQueryNode extends AbstractQueryNode implements TimeSeriesDataSource {

  public static final String M_READ_LATENCY = "events.read.latency";

  private TimeSeriesDataSourceConfig config;
  private IndexManager indexManager;

  public EventsQueryNode(EventsSourceFactory factory,
                         QueryPipelineContext context,
                         TimeSeriesDataSourceConfig config) {
    super(factory, context);
    this.config = config;
    indexManager = factory.writer().indexManager();
  }

  @Override
  public void fetchNext(Span span) {
    long start = this.pipelineContext().query().startTime().epoch();
    long end = this.pipelineContext().query().endTime().epoch();
    LuceneQueryBuilder luceneQueryBuilder = LuceneQueryBuilder.newBuilder()
        .fromTimeSeriesDataSourceConfig(config)
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(config.getFrom())
        .setSize(config.getSize() == 0 ? 10
            : config.getSize() == -1 ? Integer.MAX_VALUE : config.getSize())
        .setNamespace(config.getNamespace());

    boolean groupBy = false;
    boolean downsample = false;
    String groupByKey = null;
    for (Object node : config.getPushDownNodes()) {
      if (node instanceof GroupByConfig) {
        groupBy = true;
        Set<String> tagKeys = ((GroupByConfig) node).getTagKeys();
        groupByKey = tagKeys.iterator().next();
        break;
      }
      if (node instanceof DownsampleConfig) {
        downsample = true;
        break;
      }
    }

    luceneQueryBuilder.setGroupBy(groupByKey);

    LuceneEventsQuery eventsQuery = null;
    try {
      eventsQuery = luceneQueryBuilder.build();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot build lucene query ", e);
    }
    EventsQueryResult queryResult = new EventsQueryResult(this);

    long startTime = DateTime.nanoTime();
    if (groupBy) {
      EventsGroupResponse eventsGroupResponse = new QueryRunner(eventsQuery, indexManager)
          .searchAndGroup(eventsQuery.getGroupBy());
      indexManager.statsCollector().addTime(M_READ_LATENCY,
              DateTime.nanoTime() - startTime, ChronoUnit.NANOS,
              "type", "groupBy", "namespace", config.getNamespace());
      for (IndexGroupResponse response : eventsGroupResponse.getGroupResponse()) {
        queryResult.addSeries(response);
      }

    } else if (downsample) {
      EventsDownsampleResponse eventsDownsampleResponse = new QueryRunner(eventsQuery, indexManager)
          .searchAndDownsample();
      indexManager.statsCollector().addTime(M_READ_LATENCY,
              DateTime.nanoTime() - startTime, ChronoUnit.NANOS,
              new String[]{"type", "downsample", "namespace", config.getNamespace()});
      queryResult.addSeries(eventsDownsampleResponse);

    } else {
      EventsResponse response = new QueryRunner(eventsQuery, indexManager).searchAndFetch();
      indexManager.statsCollector().addTime(M_READ_LATENCY,
              DateTime.nanoTime() - startTime, ChronoUnit.NANOS,
              "type", "fetch", "namespace", config.getNamespace());
      List<IndexEventsResponse> events = response.getResponses();
      for (IndexEventsResponse eventsResponse : events) {
        if (eventsResponse.getEvents() != null) {
          queryResult.addSeries(eventsResponse.getEvents(), response.getHits());
        }
      }
    }

    onNext(queryResult);
    onComplete(this, 0, 0);
  }

  @Override
  public String[] setIntervals() {
    return null;
  }

  @Override
  public void onComplete(QueryNode downstream, long final_sequence, long total_sequences) {
    this.completeUpstream(final_sequence, total_sequences);
  }

  @Override
  public void onError(Throwable t) {
    sendUpstream(t);
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {

  }

  @Override
  public void onNext(QueryResult next) {
    sendUpstream(next);
  }

}
