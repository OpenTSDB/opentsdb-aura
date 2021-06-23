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
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.rollup.RollupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * A class for non-grouped results.
 *
 * ASSUMPTIONS:
 * AS will fully satisfy this part of the query, i.e. this time range.
 *
 */
public class AerospikeQueryResult implements QueryResult {
  private static final Logger logger = LoggerFactory.getLogger(AerospikeQueryResult.class);

  protected final QueryNode queryNode;
  protected final MetaTimeSeriesQueryResult metaResult;
  protected byte[] metric;
  protected String metricString;

  public AerospikeQueryResult(QueryNode queryNode, MetaTimeSeriesQueryResult metaResult) {
    this.queryNode = queryNode;
    this.metaResult = metaResult;
    metric = ((TimeSeriesDataSourceConfig) queryNode.config())
            .getMetric().getMetric().getBytes(Const.UTF8_CHARSET);
    metricString = ((TimeSeriesDataSourceConfig) queryNode.config())
            .getMetric().getMetric();
  }

  public MetaTimeSeriesQueryResult metaResult() {
    return metaResult;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return new ASTSList();
  }

  public byte[] metric() {
    return metric;
  }

  public String metricString() {
    return metricString;
  }

  @Override
  public TimeSpecification timeSpecification() {
    return null;
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

  protected class ASTSList implements List<TimeSeries> {

    @Override
    public int size() {
      return metaResult.numGroups();
    }

    @Override
    public boolean isEmpty() {
      return metaResult.numGroups() <= 0;
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public Iterator<TimeSeries> iterator() {
      logger.info("New IT: ", new RuntimeException());
      return new TSIterator();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean add(TimeSeries timeSeries) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean addAll(Collection<? extends TimeSeries> c) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean addAll(int index, Collection<? extends TimeSeries> c) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public void clear() {

    }

    @Override
    public TimeSeries get(int index) {
      // TODO - pool
      logger.info("GET " + index, new RuntimeException());
      AerospikeRawTimeSeries ts = new AerospikeRawTimeSeries(AerospikeQueryResult.this, metaResult.getGroup(index));
      return ts;
    }

    @Override
    public TimeSeries set(int index, TimeSeries element) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public void add(int index, TimeSeries element) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public TimeSeries remove(int index) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public int indexOf(Object o) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public int lastIndexOf(Object o) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public ListIterator<TimeSeries> listIterator() {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public ListIterator<TimeSeries> listIterator(int index) {
      throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public List<TimeSeries> subList(int fromIndex, int toIndex) {
      throw new UnsupportedOperationException("Not implemented.");
    }
  }

  protected class TSIterator implements Iterator<TimeSeries> {
    int index = 0;

    @Override
    public boolean hasNext() {
      return index < metaResult.numGroups();
    }

    @Override
    public TimeSeries next() {
      logger.info("Next from IT: ", new RuntimeException());
      AerospikeRawTimeSeries ts = new AerospikeRawTimeSeries(AerospikeQueryResult.this, metaResult.getGroup(index++));
      return ts;
    }
  }
}