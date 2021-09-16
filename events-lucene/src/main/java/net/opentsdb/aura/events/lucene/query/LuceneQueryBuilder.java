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

import net.opentsdb.events.EventWriter;
import net.opentsdb.events.Fields;
import net.opentsdb.events.query.QueryBuilder;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.query.filter.PassThroughFilter;
import net.opentsdb.query.filter.QueryFilter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LuceneQueryBuilder implements QueryBuilder {

  private long startTime;
  private long endTime;
  private String query;
  private int from;
  private int size;
  private String namespace;
  private String groupBy;
  private boolean fetchLast;
  private static String[] allFields = new String[]{Fields.source.name(), Fields.namespace.name(),
          Fields.userid.name(), Fields.priority.name(), "tags", "props", Fields.message.name(), Fields.title.name()};
  private static final Logger LOG = LoggerFactory.getLogger(LuceneQueryBuilder.class);

  public static LuceneQueryBuilder newBuilder() {
    return new LuceneQueryBuilder();
  }

  public LuceneEventsQuery build() throws IOException {
    if (query == null) {
      throw new IllegalStateException("Query cannot be null");
    }
    if (startTime == 0L) {
      throw new IllegalStateException("Start cannot be null");
    }
    if (endTime == 0L) {
      throw new IllegalStateException("End cannot be null");
    }

    if (query.isEmpty()) {
      query = "*:*";
    } else if (query.startsWith("NOT ") || query.startsWith("AND ") || query.startsWith("OR ")) {
      query = "*:*" + " " + query;
    }

   org.apache.lucene.queryparser.classic.QueryParser customParser = new MultiFieldQueryParser(allFields, buildAnalyzer());

    Query customQuery = null;

    try {
      customQuery = customParser.parse(query);
      LOG.info("Original Query = " + customQuery);
    } catch (ParseException e) {
      throw new IllegalStateException("Cannot parse the query", e);
    }

    customQuery = QueryParser.parse(customQuery);

    LOG.info("Parsed Query = " + customQuery);

    BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
    queryBuilder.add(customQuery, Occur.MUST);

    BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
    booleanQueryBuilder.add(queryBuilder.build(), Occur.MUST);
    booleanQueryBuilder.add(getTimeRangeQuery(), Occur.MUST);
    booleanQueryBuilder.add(getNamespaceQuery(), Occur.MUST);

    Set<String> indicesFromTime = getIndicesFromTime(startTime, endTime);
    List<IndexQuery> queryPerIndex = new ArrayList<>();
    BooleanQuery query = booleanQueryBuilder.build();
    for (String index : indicesFromTime) {
      IndexQuery iq = new IndexQuery(index, query, from, size);
      queryPerIndex.add(iq);
    }

    return new LuceneEventsQuery(queryPerIndex, ("Tag."+groupBy).toLowerCase(), fetchLast);
  }

  private Query getNamespaceQuery() {
    Query query = new TermQuery(new Term("namespace", namespace.toLowerCase()));
    return query;
  }

  private BooleanQuery getTimeRangeQuery() {
    BooleanQuery.Builder tsBooleanQuery = new BooleanQuery.Builder();
    tsBooleanQuery
        .add(LongPoint.newRangeQuery(Fields.starttimestamp.name(), startTime, endTime), Occur.SHOULD);
    tsBooleanQuery
        .add(LongPoint.newRangeQuery(Fields.endtimestamp.name(), startTime, endTime), Occur.SHOULD);
    tsBooleanQuery.add(IntPoint.newExactQuery(Fields.ongoing.name(), 1), Occur.SHOULD);
    return tsBooleanQuery.build();
  }

  public Set<String> getIndicesFromTime(long start, long end) { // startTime and endTime in seconds
    Set<String> indices = new HashSet<>();
    String startIndex = EventWriter.getIndexName(start);
    indices.add(startIndex);
    String endIndex = EventWriter.getIndexName(end);
    if (startIndex.equalsIgnoreCase(endIndex)) {
      return indices;
    }
    LocalDate endDate = LocalDate.parse(endIndex, EventWriter.df);
    String currentIndex = startIndex;
    LocalDate current = fetchNextIndex(currentIndex);
    while (current.isBefore(endDate) || current.isEqual(endDate)) {
      currentIndex = current.toString();
      indices.add(currentIndex);
      current = fetchNextIndex(currentIndex);
    }
    return indices;
  }

  private LocalDate fetchNextIndex(String currentIndex) {
    return LocalDate.parse(currentIndex).plusDays(1);
  }

  public LuceneQueryBuilder fromTimeSeriesDataSourceConfig(TimeSeriesDataSourceConfig config) {

    QueryFilter filter = config.getFilter();
    if (filter == null) {
      this.query = "*:*";
    }
    if (filter instanceof ChainFilter) {
      if (((ChainFilter) filter).getOp() == FilterOp.AND) {
        for (final QueryFilter subFilter : ((ChainFilter) filter).getFilters()) {
          if (subFilter instanceof PassThroughFilter) {
            String query = ((PassThroughFilter) subFilter).getFilter().trim();
            this.query = query;
          }
        }
      }
    }
    this.namespace = config.getNamespace();
    this.fetchLast = config.getFetchLast();
    return this;
  }

  public LuceneQueryBuilder setQuery(String query) {
    this.query = query;
    return this;
  }

  public LuceneQueryBuilder setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public LuceneQueryBuilder setEndTime(long endTime) {
    this.endTime = endTime;
    return this;
  }

  public LuceneQueryBuilder setFrom(int from) {
    this.from = from;
    return this;
  }

  public LuceneQueryBuilder setSize(int size) {
    this.size = size;
    return this;
  }

  public LuceneQueryBuilder setNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }


  public LuceneQueryBuilder setGroupBy(String groupBy) {
    this.groupBy = groupBy;
    return this;
  }

  public static Analyzer buildAnalyzer() throws IOException {
    return CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("lowercase")
        .build();

  }

}
