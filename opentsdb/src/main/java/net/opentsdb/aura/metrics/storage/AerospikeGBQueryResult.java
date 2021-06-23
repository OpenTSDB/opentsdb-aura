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

import com.google.common.reflect.TypeToken;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.common.Const;
import net.opentsdb.data.*;
import net.opentsdb.query.*;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.AerospikeGBTimeSeries;
import net.opentsdb.rollup.RollupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Iterator;
import java.util.List;

/**
 * ASSUMPTIONS:
 * AS will fully satisfy this part of the query, i.e. this time range.
 *
 */
public class AerospikeGBQueryResult extends AerospikeQueryResult implements TimeSpecification {
  private static final Logger logger = LoggerFactory.getLogger(AerospikeGBQueryResult.class);

  protected DownsampleConfig downsampleConfig;
  private byte[] metric; // set by TimeSeriesStorage.query.

  public AerospikeGBQueryResult(QueryNode queryNode, MetaTimeSeriesQueryResult metaResult) {
    super(queryNode, metaResult);
    downsampleConfig = ((AerospikeQueryNode) queryNode).downsampleConfig();
    metric = ((TimeSeriesDataSourceConfig) queryNode.config())
            .getMetric().getMetric().getBytes(Const.UTF8_CHARSET);
  }

  public MetaTimeSeriesQueryResult metaResult() {
    return metaResult;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return new ASGBList();
  }

  @Override
  public TimeStamp start() {
    return downsampleConfig.startTime();
  }

  @Override
  public TimeStamp end() {
    return downsampleConfig.endTime();
  }

  @Override
  public TemporalAmount interval() {
    return downsampleConfig.interval();
  }

  @Override
  public String stringInterval() {
    return downsampleConfig.getInterval();
  }

  @Override
  public ChronoUnit units() {
    return downsampleConfig.units();
  }

  @Override
  public ZoneId timezone() {
    return downsampleConfig.timezone();
  }

  @Override
  public void updateTimestamp(int offset, TimeStamp timestamp) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  @Override
  public void nextTimestamp(TimeStamp timestamp) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  public byte[] metric() {
    return metric;
  }

  public DownsampleConfig downsampleConfig() {
    return downsampleConfig;
  }

  @Override
  public TimeSpecification timeSpecification() {
    return this;
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
    return ((TimeSeriesDataSourceFactory) ((AbstractQueryNode) queryNode).factory()).rollupConfig();
  }

  @Override
  public void close() {

  }

  @Override
  public boolean processInParallel() {
    return true;
  }

  class ASGBList extends ASTSList {
    @Override
    public Iterator<TimeSeries> iterator() {
      return new TSGBIterator();
    }

    @Override
    public TimeSeries get(int index) {
      // TODO - pool
      AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(AerospikeGBQueryResult.this, metaResult.getGroup(index));
      ts.process();
      return ts;
    }
  }

  protected class TSGBIterator implements Iterator<TimeSeries> {
    int index = 0;

    @Override
    public boolean hasNext() {
      return index < metaResult.numGroups();
    }

    @Override
    public TimeSeries next() {
      AerospikeGBTimeSeries ts = new AerospikeGBTimeSeries(AerospikeGBQueryResult.this, metaResult.getGroup(index++));
      ts.process();
      return ts;
    }
  }
}