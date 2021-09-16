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

package net.opentsdb.aura.events.lucene;

import net.opentsdb.aura.events.ConfigUtils;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.events.EventWriter;
import net.opentsdb.events.view.Event;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestEventWriter {

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

  public Event getChildEvent() {
    Event event = new Event();
    event.setSource("yoyo");
    event.setTitle("Hello World child");
    event.setMessage("Goodbye World child");
    event.setNamespace("Test");
    event.setPriority("HIGH");
    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("Host", "host1");
    event.setDimensions(dimensions);
    event.setTimestamp(1563389778);
    event.setParentId(getEvent().getEventId());
    return event;
  }

  @Test
  public void testAppHasAGreeting() throws Exception {
    deleteDirectory(new File(tempPath));
    Event event = getEvent();
    event.setEventId(EventWriter.generateEventID(event));
    writer.index(event);

    Thread.sleep(10000);

    String index = writer.getIndexName(event);

    Directory dir = FSDirectory
        .open(Paths.get(tempPath + index));
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    TermQuery tQuery = new TermQuery(new Term("source", "yoyo"));
    TopDocs hits = searcher.search(tQuery, 1);
    System.out.println(hits.scoreDocs.length);
    System.out.println(hits.totalHits.value);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int doc = hits.scoreDocs[i].doc;
      System.out.println(searcher.getIndexReader().document(doc));

    }
  }

  @Test
  public void getIndexName() {
    String indexName = EventWriter.getIndexName(getEvent());
    assertEquals("2019-07-18", indexName);
  }

  public Event getEvent() {
    Event event = new Event();
    event.setSource("yoyo");
    event.setTitle("Hello World parent");
    event.setMessage("Goodbye World");
    event.setNamespace("Test");
    event.setPriority("HIGH");
    Map<String, String> dimensions = new HashMap<>();
    dimensions.put("Host", "host1");
    event.setDimensions(dimensions);
    event.setTimestamp(1563472395);
    return event;
  }

  @Test
  public void generateEventId() {
    Event event = getEvent();
    event.setEventId(EventWriter.generateEventID(event));
    String eventId = event.getEventId();
    assertEquals("2019-07-18_1569669054496456267", eventId);
  }

  @Test
  public void getIndexNameFromID() {
    Event event = getEvent();
    event.setEventId(EventWriter.generateEventID(event));
    assertEquals("2019-07-18", EventWriter.getIndexName(event.getEventId()));
  }

 // @Test
  public void testParentChild() throws Exception {
    System.out.println("DELETING DIR ");
    System.out.println(deleteDirectory(new File(tempPath)));
    Event event = getEvent();
    String parentId = EventWriter.generateEventID(event);
    event.setEventId(parentId);

    writer.index(event);

    Thread.sleep(10000);

    String index = EventWriter.getIndexName(event);
    Directory dir = FSDirectory
        .open(Paths.get(tempPath + index));
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    TermQuery q = new TermQuery(new Term("source", "yoyo"));
    TopDocs hits = searcher.search(q, 1);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int doc = hits.scoreDocs[i].doc;
      System.out.println(searcher.getIndexReader().document(doc));

    }
    assertEquals(1, hits.scoreDocs.length);

    Event child = getChildEvent();
    String childId = EventWriter.generateEventID(child);
    child.setEventId(childId);
    child.setParentId(parentId);
    writer.index(child);
    Thread.sleep(10000);

    q = new TermQuery(new Term("title", "child"));
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(q, 10);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int doc = hits.scoreDocs[i].doc;
      Document document = searcher.getIndexReader().document(doc);
      System.out.println(document);
      assertEquals(childId, document.get("eventId"));
      assertEquals(parentId, document.get("parentId"));
      assertNull(document.get("childId"));
    }

    q = new TermQuery(new Term("title", "parent"));
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(q, 10);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int doc = hits.scoreDocs[i].doc;
      Document document = searcher.getIndexReader().document(doc);
      System.out.println(document);
      assertEquals(parentId, document.get("eventId"));
      assertEquals(childId, document.get("childId"));
      assertNull(document.get("parentId"));
    }
  }

 // @Test
  public void testParentMultipleChild() throws Exception {
    deleteDirectory(new File(tempPath));
    Event event = getEvent();
    String parentId = EventWriter.generateEventID(event);
    event.setEventId(parentId);

    writer.index(event);

    Thread.sleep(10000);

    String index = EventWriter.getIndexName(event);
    Directory dir = FSDirectory
        .open(Paths.get(tempPath + index));
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    TermQuery q = new TermQuery(new Term("source", "yoyo"));
    TopDocs hits = searcher.search(q, 1);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int doc = hits.scoreDocs[i].doc;
      System.out.println(searcher.getIndexReader().document(doc));

    }
    assertEquals(1, hits.scoreDocs.length);

    // First child
    Event child = getChildEvent();
    child.setTitle("child 1");
    String childId = EventWriter.generateEventID(child);
    child.setEventId(childId);
    child.setParentId(parentId);
    writer.index(child);

    // Second child
    child = getChildEvent();
    child.setTitle("child 2");
    child.setTimestamp(1200000200L);
    String childId2 = EventWriter.generateEventID(child);
    child.setEventId(childId2);
    child.setParentId(parentId);
    writer.index(child);

    Thread.sleep(10000);

    q = new TermQuery(new Term("title", "child 2"));
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(q, 10);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int doc = hits.scoreDocs[i].doc;
      Document document = searcher.getIndexReader().document(doc);
      System.out.println(document);
      assertEquals(childId, document.get("eventId"));
      assertEquals(parentId, document.get("parentId"));
      assertNull(document.get("childId"));
    }

    q = new TermQuery(new Term("title", "parent"));
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(q, 10);
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      int doc = hits.scoreDocs[i].doc;
      Document document = searcher.getIndexReader().document(doc);
      System.out.println(document);
      assertEquals(parentId, document.get("eventId"));
      assertEquals(2, document.getValues("childId").length);
      List<String> children = Arrays.asList(document.getValues("childId"));
      assertTrue(children.contains(childId));
      assertTrue(children.contains(childId2));
      assertNull(document.get("parentId"));
    }
  }

  @Test
  public void createIndex() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      Thread t = new Thread(() -> {
        try {
          System.out.println("indexing");
          writer.index(getEvent());
        } catch (Throwable throwable) {
          throwable.printStackTrace();
        }

      });
      t.start();
    }
    Thread.sleep(10000);
  }

}
