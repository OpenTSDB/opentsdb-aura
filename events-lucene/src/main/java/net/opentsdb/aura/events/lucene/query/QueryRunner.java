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

import com.google.common.base.Strings;
import net.opentsdb.aura.events.lucene.EventDocumentTranslater;
import net.opentsdb.aura.events.lucene.IndexManager;
import net.opentsdb.events.Fields;
import net.opentsdb.events.query.EventsDownsampleResponse;
import net.opentsdb.events.query.EventsGroupResponse;
import net.opentsdb.events.query.EventsResponse;
import net.opentsdb.events.query.Group;
import net.opentsdb.events.query.IndexEventsResponse;
import net.opentsdb.events.query.IndexGroupResponse;
import net.opentsdb.events.view.Event;
import net.opentsdb.utils.DateTime;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.apache.lucene.search.grouping.TopGroupsCollector;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class QueryRunner {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRunner.class);

  public static final String M_READ_LATENCY = "events.read.index.latency";

  private final LuceneEventsQuery luceneEventsQuery;
  private final IndexManager indexManager;
  private double maxRAMMB = 1024D;
  private String no_group = "_NO_GROUP";
  private String sortField = Fields.starttimestamp.name() + "-sort";
  private int maxDocsPerGroup = 1;

  public QueryRunner(LuceneEventsQuery luceneEventsQuery, final IndexManager indexManager) {
    this.luceneEventsQuery = luceneEventsQuery;
    this.indexManager = indexManager;
  }

  public EventsResponse searchAndFetch() {
    LOG.info("Running query " + luceneEventsQuery);
    long start = System.currentTimeMillis();
    List<IndexQuery> luceneQueryPerIndex = luceneEventsQuery.getLuceneQueryPerIndex();
    // Search phase
    List<LuceneIndexResponse> responses = luceneQueryPerIndex.parallelStream()
        .map(indexQuery -> {
          try {
            return searchInIndex(indexQuery);
          } catch (IOException e) {
            return new LuceneIndexResponse(indexQuery.getIndex(), null, null);
          }
        }).collect(Collectors.toList());
    Collections.sort(responses, Collections.reverseOrder());

    // fetch phase
    Map<LuceneIndexResponse, Long> indicesToFetch = new HashMap<>();
    long size = luceneQueryPerIndex.get(0).getSize();
    int i = 0;
    while (size > 0 && i < responses.size()) {
      LuceneIndexResponse resp = responses.get(i);
      if (resp.getTopDocs() != null) {
        long hits = resp.getTopDocs().totalHits.value;
        size = size - hits;
        if (size >= 0) {
          indicesToFetch.put(resp, hits);
        } else {
          indicesToFetch.put(resp, (hits + size));
        }
      }
      i++;
    }

    List<IndexEventsResponse> eventsResponses = indicesToFetch.entrySet().parallelStream()
        .map(indexToFetch -> {
          return fetch(indexToFetch.getKey(), indexToFetch.getValue());
        }).sorted(Comparator.comparing((IndexEventsResponse eventsResponse) -> eventsResponse.getIndex()).reversed()).collect(Collectors.toList());
    long hits = 0;
    for (LuceneIndexResponse resp : responses) {
      if (resp.getTopDocs() != null) {
        hits = hits + resp.getTopDocs().totalHits.value;
      }
    }

    LOG.info("Time taken for response " + (System.currentTimeMillis() - start) + " ms from "
        + luceneQueryPerIndex.size() + " indices");
    return new EventsResponse(eventsResponses, hits);
  }

  private LuceneIndexResponse searchInIndex(IndexQuery indexQuery) throws IOException {
    long start = DateTime.nanoTime();
    String index = indexQuery.getIndex();
    Query query = indexQuery.getQuery();
    int from = indexQuery.getFrom();
    int size = indexQuery.getSize();
    IndexSearcher searcher = indexManager.getSearcher(index);
    Sort sort = new Sort(new SortField(Fields.starttimestamp.name() + "-sort", Type.LONG, true));
    TopDocs results = searcher.search(query, from + size, sort);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found results in lucene from index " + index + " and " + results.totalHits.value);
    }
    indexManager.statsCollector().addTime(M_READ_LATENCY,
            DateTime.nanoTime() - start, ChronoUnit.NANOS,
            "type", "search", "index", index);
    return new LuceneIndexResponse(index, results, searcher);
  }

  public IndexEventsResponse fetch(LuceneIndexResponse indexToFetch, long count) {
    long time = System.currentTimeMillis();
    int from = 0;
    IndexSearcher searcher = indexToFetch.getSearcher();
    TopDocs results = indexToFetch.getTopDocs();
    List<Event> events = new ArrayList<>();
    for (int i = from; i < count; i++) {
      Document doc = null;
      try {
        doc = searcher.doc(results.scoreDocs[i].doc);
      } catch (IOException e) {
        return new IndexEventsResponse(new ArrayList<>(), 0, indexToFetch.getIndex());
      }
      events.add(EventDocumentTranslater.docToEvent(doc));
    }
    LOG.info("Total hits fetched from lucene " + results.totalHits + " in time " + (
        System.currentTimeMillis() - time) + " ms");
    return new IndexEventsResponse(events, results.totalHits.value, indexToFetch.getIndex());
  }


  public EventsDownsampleResponse searchAndDownsample() {
    EventsGroupResponse eventsGroupResponse = searchAndGroup(Fields.timestampMinute.name());
    return new EventsDownsampleResponse(eventsGroupResponse.getGroupResponse());
  }

  public EventsGroupResponse searchAndGroup(String groupBy) {
    LOG.info("Running query: " + luceneEventsQuery);

    if (Strings.isNullOrEmpty(groupBy)) {
      throw new IllegalStateException("GroupBy field cannot be empty");
    }

    Map<Group, IndexGroupResponse> groupsResponse = new HashMap<>();

    boolean fetchLast = luceneEventsQuery.fetchLast();
    Map<String, List<IndexGroupResponse>> responsePerIndex = luceneEventsQuery
        .getLuceneQueryPerIndex()
        .parallelStream()
        .map(q -> groupInIndex(groupBy, q, fetchLast))
        .filter(q -> q.size() > 0)
        .collect(Collectors.toMap(k -> k.get(0).getIndex(), v -> v));

    responsePerIndex
        .forEach((index, groupResponse) ->
            groupResponse.forEach(gr -> {
              Group group = gr.getGroups();
              IndexGroupResponse indexGroupResponse = groupsResponse.get(group);
              if (indexGroupResponse != null) {
            indexGroupResponse.addHits(gr.getHits());
            if (gr.getEvent() != null) {
              indexGroupResponse.addEvent(gr.getEvent());
            }
          } else {
            groupsResponse
                .put(group, gr);
          }
        }));

    return new EventsGroupResponse(new TreeSet<>(groupsResponse.values()));
  }

  private List<IndexGroupResponse> groupInIndex(String groupBy, IndexQuery q, boolean fetchLast) {
    long start = DateTime.nanoTime();
    List<IndexGroupResponse> groupsInIndex = new ArrayList<>();
    String groupByKey = groupBy.replaceFirst("tag.", "");
    try {
      IndexSearcher searcher = indexManager.getSearcher(q.getIndex());
      Sort sortWithinGroup = new Sort(
          new SortField(sortField, Type.LONG,
              true));
      TermGroupSelector termGroupSelector = new TermGroupSelector(groupBy);
      AllGroupsCollector firstPassCollector = new AllGroupsCollector(
          termGroupSelector);

      CachingCollector cachedCollector = CachingCollector
          .create(firstPassCollector, false, maxRAMMB);
      searcher.search(q.getQuery(), cachedCollector);
      Collection<BytesRef> groups = firstPassCollector.getGroups();
      if (groups.isEmpty()) {
        return groupsInIndex;
      }
      Set<SearchGroup> topGroups = new HashSet<>();
      // AllGroupsCollector does not give SearchGroup with is needed by the SecondPass
      // Hence create a SearchGroup for each group.
      // (can be avoided if FirstPassCollector is used)
      for (BytesRef ref : groups) {
        SearchGroup sg = new SearchGroup();
        sg.groupValue = ref;
        topGroups.add(sg);
      }
      if (topGroups != null) {
        TopGroupsCollector secondPassCollector
            = new TopGroupsCollector(termGroupSelector, topGroups, sortWithinGroup,
            sortWithinGroup,
            maxDocsPerGroup, false);
        if (cachedCollector.isCached()) {
          cachedCollector.replay(secondPassCollector);
        } else {
          searcher.search(q.getQuery(), secondPassCollector);
        }

        for (GroupDocs<BytesRef> doc : secondPassCollector.getTopGroups(0).groups) {
          Document document = searcher.doc(doc.scoreDocs[0].doc);
          BytesRef br = doc.groupValue;
          String groupValue = br == null ? null : br.utf8ToString();
          Event event = null;
          if (fetchLast) {
            event = EventDocumentTranslater.docToEvent(document);
          }
          group(groupByKey, groupValue, q.getIndex(), doc.totalHits.value, event, groupsInIndex);
        }
      }

    } catch (IOException e) {
      // no data for this index
    }
    indexManager.statsCollector().addTime(M_READ_LATENCY,
            DateTime.nanoTime() - start, ChronoUnit.NANOS,
            "type", "group", "index", q.getIndex());
    return groupsInIndex;
  }

  private void group(String groupByKey, String groupValue, String index, long hits,
      Event event, List<IndexGroupResponse> groupsInIndex) {

    boolean downsample = false;
    if (groupByKey.equalsIgnoreCase(Fields.timestampMinute.name())) {
      downsample = true;
    }
    if (groupValue == null) {
      Group g = new Group(groupByKey, no_group);
      groupsInIndex.add(new IndexGroupResponse(index, g, hits, event, downsample));
    } else {
      Group group = new Group(groupByKey, groupValue);
      groupsInIndex.add(new IndexGroupResponse(index, group, hits, event, downsample));
    }
  }
}

class LuceneIndexResponse implements Comparable<LuceneIndexResponse> {

  private TopDocs topDocs;

  private IndexSearcher searcher;
  private String index;

  public LuceneIndexResponse(String index, TopDocs topDocs, IndexSearcher searcher) {
    this.topDocs = topDocs;
    this.searcher = searcher;
    this.index = index;
  }

  public String getIndex() {
    return index;
  }

  public TopDocs getTopDocs() {
    return topDocs;
  }

  public IndexSearcher getSearcher() {
    return searcher;
  }

  @Override
  public int compareTo(LuceneIndexResponse o) {
    return LocalDate.parse(this.index).compareTo(LocalDate.parse(o.index));
  }

  @Override
  public String toString() {
    return ", index='" + index + '\'' +
        '}';
  }
}
