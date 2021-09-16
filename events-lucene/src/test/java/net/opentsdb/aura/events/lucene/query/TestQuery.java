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

import net.opentsdb.aura.events.ConfigUtils;
import net.opentsdb.aura.events.lucene.LuceneWriter;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.events.EventWriter;
import net.opentsdb.events.query.EventsDownsampleResponse;
import net.opentsdb.events.query.EventsGroupResponse;
import net.opentsdb.events.query.EventsResponse;
import net.opentsdb.events.query.IndexEventsResponse;
import net.opentsdb.events.view.Event;
import org.apache.lucene.index.IndexWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestQuery {

  private LuceneWriter writer;
  private String tempPath;

  @BeforeEach
  public void before() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    ConfigUtils.registerConfigs(tsdb, null);

    writer = new LuceneWriter();
    writer.initialize(tsdb, null).join(30_000);
    tempPath = writer.indexManager().dataPath();

    for (IndexWriter writer : writer.indexManager().getAllWriters().values()) {
      writer.close();
    }
    writer.indexManager().getAllWriters().clear();
    writer.indexManager().getAllReaders().clear();
    deleteDirectory(new File(tempPath));
  }

  @AfterEach
  public void after() throws Exception {
    writer.shutdown().join();
  }

  boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  @Test
  public void getIndicesFromQuery() {
    long start = 1563221605;
    long end = 1563308005;
    LuceneQueryBuilder query = LuceneQueryBuilder.newBuilder().setQuery("")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(10);
    Set<String> indicesFromTime = query.getIndicesFromTime(start, end);
    assertTrue(indicesFromTime.contains("2019-07-15"));
    assertTrue(indicesFromTime.contains("2019-07-16"));
  }

  @Test
  public void queryBuilder() throws IOException {
    long start = 1563221605L;
    long end = 1563308005L;
    LuceneQueryBuilder query = LuceneQueryBuilder.newBuilder().setQuery("title:hello AND low")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(1)
        .setNamespace("namespace");
    LuceneEventsQuery build = query.build();
    System.out.println(build.getLuceneQueryPerIndex());
    assertEquals(2, build.getLuceneQueryPerIndex().size());

  }

  @Test
  public void query() throws Exception {
    long start = 1563222405;
    long end = 1563222405 + (99 * 86400);
    writeEvents(100, "host1");
    Thread.sleep(10000);
    LuceneQueryBuilder query = LuceneQueryBuilder.newBuilder().setQuery("\\**hello*")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(10)
        .setNamespace("namespace");
    List<IndexQuery> luceneQueryPerIndex = query.build().getLuceneQueryPerIndex();
    QueryRunner runner = new QueryRunner(new LuceneEventsQuery(
            luceneQueryPerIndex, null, false),
            writer.indexManager());
    EventsResponse response = runner.searchAndFetch();
    List<IndexEventsResponse> run = response.getResponses();
    System.out.println(run);
    assertEquals(4, run.size());
   // System.out.println(run);

    query = LuceneQueryBuilder.newBuilder().setQuery("tag.host\\:hello:Host1")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(10)
        .setNamespace("namespace");
    luceneQueryPerIndex = query.build().getLuceneQueryPerIndex();
   // System.out.println(luceneQueryPerIndex);
    runner = new QueryRunner(
            new LuceneEventsQuery(luceneQueryPerIndex, null, false),
            writer.indexManager());
    response = runner.searchAndFetch();
    run = response.getResponses();
    System.out.println(run);
    assertEquals(6, run.size());
    System.out.println(run);

    query = LuceneQueryBuilder.newBuilder().setQuery("tag.host\\:hello:Host1 AND source:aws\\/autoscaling")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(10)
        .setNamespace("namespace");
    luceneQueryPerIndex = query.build().getLuceneQueryPerIndex();
    // System.out.println(luceneQueryPerIndex);
    runner = new QueryRunner(
            new LuceneEventsQuery(luceneQueryPerIndex, null, false),
            writer.indexManager());
    response = runner.searchAndFetch();
    run = response.getResponses();
    System.out.println(run);
    assertEquals(6, run.size());
    System.out.println(run);

    query = LuceneQueryBuilder.newBuilder().setQuery("host1")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(10)
        .setNamespace("namespace");
    luceneQueryPerIndex = query.build().getLuceneQueryPerIndex();
    System.out.println(luceneQueryPerIndex);
    runner = new QueryRunner(
            new LuceneEventsQuery(luceneQueryPerIndex, null, false),
            writer.indexManager());
    response = runner.searchAndFetch();
    run = response.getResponses();
    assertEquals(6, run.size());
    System.out.println(run);

    query = LuceneQueryBuilder.newBuilder().setQuery("*:*")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(10)
        .setNamespace("namespace");
    luceneQueryPerIndex = query.build().getLuceneQueryPerIndex();
    System.out.println(luceneQueryPerIndex);
    runner = new QueryRunner(
            new LuceneEventsQuery(luceneQueryPerIndex, null, false),
            writer.indexManager());
    response = runner.searchAndFetch();
    run = response.getResponses();
    assertEquals(4, run.size());
    System.out.println(run);

    query = LuceneQueryBuilder.newBuilder().setQuery("tag.host\\:hello:Host1 AND source:/scal/")
            .setStartTime(start)
            .setEndTime(end)
            .setFrom(0)
            .setSize(10)
            .setNamespace("namespace");
    luceneQueryPerIndex = query.build().getLuceneQueryPerIndex();
     System.out.println(luceneQueryPerIndex);
    runner = new QueryRunner(
            new LuceneEventsQuery(luceneQueryPerIndex, null, false),
            writer.indexManager());
    response = runner.searchAndFetch();
    run = response.getResponses();
    System.out.println(run);
    assertEquals(6, run.size());
    System.out.println(run);

  }

  @Test
  public void testGrouping() throws Exception {
    long start = 1563222405;
    long end = 1563222405 + (1 * 86400);
    writeEvents(2, "host1");
    Thread.sleep(20000);
    LuceneQueryBuilder query = LuceneQueryBuilder.newBuilder().setQuery("source:aws*")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(10)
        .setGroupBy("host:hello")
        .setNamespace("namespace");
    LuceneEventsQuery build = query.build();
    QueryRunner runner = new QueryRunner(build, writer.indexManager());
    EventsGroupResponse eventsGroupResponse = runner.searchAndGroup(build.getGroupBy());
    System.out.println(eventsGroupResponse);
    assertEquals(3, eventsGroupResponse.getGroupResponse().size());
  }

  @Test
  public void testServiceNowEvents() throws Exception {
    long start = 1563222405;
   // Thread.sleep(20000);
    Event createEvent = createEvent(start, "host1");
    createEvent.setSource("servicenow");
    createEvent.getDimensions().put("Id", "INC1674657");
    createEvent.getAdditionalProps().put(Event.UPDATE_ID, "INC1674657");
    Event updateEvent = createEvent(start, "host1");
    updateEvent.setUpdate(true);
    updateEvent.setSource("servicenow");
    updateEvent.setTitle("Updating Title");
    updateEvent.setEventId(null);
    updateEvent.getDimensions().put("Id", "INC1674657");
    updateEvent.getAdditionalProps().put(Event.UPDATE_ID, "INC1674657");
    updateEvent.getAdditionalProps().put(Event.UPSERT_KEY, true);

    writer.index(createEvent);

    Thread.sleep(5000);


    LuceneQueryBuilder query = LuceneQueryBuilder.newBuilder().setQuery("*:*")
        .setStartTime(start)
        .setEndTime(start + 60000)
        .setFrom(0)
        .setSize(10)
        .setNamespace("namespace");
    LuceneEventsQuery build = query.build();
    QueryRunner runner = new QueryRunner(build, writer.indexManager());
    EventsResponse eventsGroupResponse = runner.searchAndFetch();
    System.out.println(eventsGroupResponse.getResponses());
    writer.index(updateEvent);
    //writer.index(updateEvent);

    query = LuceneQueryBuilder.newBuilder().setQuery("source:servicenow")
            .setStartTime(start)
            .setEndTime(start + 60000)
            .setFrom(0)
            .setSize(10)
            .setNamespace("namespace");
    build = query.build();
    runner = new QueryRunner(build, writer.indexManager());

    eventsGroupResponse = runner.searchAndFetch();
    System.out.println(eventsGroupResponse.getResponses());
    assertEquals(1, eventsGroupResponse.getHits());
    assertEquals("Updating Title", eventsGroupResponse.getResponses().get(0).getEvents().get(0).getTitle());
  }

  @Test
  public void testDownsample() throws Exception {
    long start = 1563222405;
    long end = 1563222405 + (1 * 86400);
    writeEvents(2, "host1");
    Thread.sleep(10000);
    LuceneQueryBuilder query = LuceneQueryBuilder.newBuilder().setQuery("source:aws*")
        .setStartTime(start)
        .setEndTime(end)
        .setFrom(0)
        .setSize(10)
        .setGroupBy("host123")
        .setNamespace("Namespace");
    LuceneEventsQuery build = query.build();
    QueryRunner runner = new QueryRunner(build, writer.indexManager());
    EventsDownsampleResponse eventsGroupResponse = runner.searchAndDownsample();
    System.out.println(eventsGroupResponse);
    assertEquals(5, eventsGroupResponse.getGroupResponse().size());
  }

  @Test
  public void testSpecialCharParsing() throws IOException {
    long start = 1563222405;
    long end = 1563222405 + (99 * 86400);
    String s = "+ - && || ! ( ) { } [ ] ^ \" ~ * ? :";
    String[] split = s.split("\\s");
    List<String> luceneSpecial =Arrays.asList(split);
    for (int i = 33; i < 48; i++) {
      if ((char) i == '/') continue;
      String c = Character.toString((char) i);
      if (! luceneSpecial.contains(c)) {
        LuceneQueryBuilder query = LuceneQueryBuilder.newBuilder().setQuery(c)
            .setStartTime(start)
            .setEndTime(end)
            .setFrom(0)
            .setSize(10)
            .setNamespace("namespace");
        List<IndexQuery> luceneQueryPerIndex = query.build().getLuceneQueryPerIndex();
        QueryRunner runner = new QueryRunner(
            new LuceneEventsQuery(luceneQueryPerIndex, null, false),
                writer.indexManager());
      }
    }
  }

  public void writeEvents(int size, String host) throws Exception {
    System.setProperty("test", "true");
//    Event event = createEvent(1563222405);
//    writer.index(event);
//    event = createEvent(1563306005);
//    writer.index(event);

    long ts = 1563222405;
    for (int i = 0; i < size ; i++) {
      Event event = createEvent(ts, host);
      writer.index(event);
      writer.index(createEvent(ts + 60, "host1"));
      writer.index(createEvent(ts + 120, "host2"));
      writer.index(createEvent(ts + 180, "host3"));
      ts = ts + 86400;
    }
  }

  public Event createEvent(long ts, String host) {
    Event event = new Event();
    Map<String, String> tags = new HashMap<>();
    tags.put("host:hello", host);
    tags.put("colo", "colo1");
    event.setDimensions(tags);

    Map<String, Object> props = new HashMap<>();
    props.put("status", "warning");
    event.setAdditionalProps(props);

    event.setNamespace("Namespace");
    event.setSource("AWS/AutoScaling");
    event.setTitle("Hello-world");
    event.setMessage("**Hello**: World");
    event.setTimestamp(ts);

    String eventId = EventWriter.generateEventID(event);
    event.setEventId(eventId);
    return event;
  }

}
