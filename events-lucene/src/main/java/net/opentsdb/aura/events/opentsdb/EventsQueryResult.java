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

import com.google.common.reflect.TypeToken;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.events.query.IndexGroupResponse;
import net.opentsdb.events.view.Event;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.rollup.RollupConfig;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class EventsQueryResult  implements QueryResult {

  private final QueryNode queryNode;
  private final List<TimeSeries> series;


  public EventsQueryResult(QueryNode queryNode) {
    this.queryNode = queryNode;
    this.series = new ArrayList<>();
  }

  @Override
  public TimeSpecification timeSpecification() {
    return null;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return series;
  }

  @Override
  public String error() {
    return null;
  }

  @Override
  public Throwable exception() {
    return null;
  }

  @Override
  public long sequenceId() {
    return 0;
  }

  @Override
  public QueryNode source() {
    return queryNode;
  }

//  @Override
//  public String dataSource() {
//    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) queryNode.config();
//    return (config. == null || config.getDataSourceId().isEmpty()) ? config.getId() : config.getDataSourceId();
//  }
  @Override
  public QueryResultId dataSource() {
    return (QueryResultId) queryNode.config().resultIds().get(0);
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public ChronoUnit resolution() {
     return ChronoUnit.SECONDS;
  }

  @Override
  public RollupConfig rollupConfig() {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean processInParallel() {
    return false;
  }

  public void addSeries(List<Event> events, long hits) {
    for (Event event : events) {
      this.series.add(new EventsTimeseries(event, hits));
    }
  }

  public void addSeries(IndexGroupResponse response) {
    this.series.add(new EventsTimeseries(response));
  }

  public void addSeries(TimeSeries ts) {
    this.series.add(ts);
  }


}
