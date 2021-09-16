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

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.events.ConfigUtils;
import net.opentsdb.core.TSDB;
import net.opentsdb.events.EventWriter;
import net.opentsdb.events.Fields;
import net.opentsdb.events.view.Event;
import net.opentsdb.service.TSDBService;
import net.opentsdb.stats.StatsCollector;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static net.opentsdb.aura.events.lucene.EventDocumentTranslater.addAdditionalProps;
import static net.opentsdb.aura.events.lucene.EventDocumentTranslater.addStringFieldIfPresent;
import static net.opentsdb.aura.events.lucene.EventDocumentTranslater.addTextFieldIfPresent;
import static net.opentsdb.aura.events.lucene.EventDocumentTranslater.addTimestamp;
import static net.opentsdb.aura.events.lucene.EventDocumentTranslater.eventToDoc;

public class LuceneWriter extends EventWriter implements TSDBService {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneWriter.class);

  public static final String TYPE = "LuceneEventsWriter";
  public static final String M_INDEXED = "events.indexed";

  public static final String M_UPDATES_SKIPPED = "events.updates.skipped";
  public static final String M_INDEX_ERRORS = "events.indices.errors";

  private final String[] updateEventsTags = new String[]{"type", "update"};

  private IndexManager indexManager;
  private TSDB tsdb;
  private StatsCollector stats;
  private String id;

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    stats = tsdb.getStatsCollector();
    ConfigUtils.registerConfigs(tsdb, id);

    indexManager = new IndexManager(this);
    indexManager.start();
    LOG.info("Creating a new lucene writer");

    tsdb.getRegistry().registerSharedObject(Strings.isNullOrEmpty(id) ?
            TYPE : TYPE + "_" + id, this);
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    indexManager.shutdown();
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public void index(Event event) throws Exception {
    String index = getIndexName(event);
    IndexWriter writer = indexManager.getIndexWriter(index);

    if (!event.isUpdate()) { // new Event
      doCreate(event, writer);
    } else {
      // update
      stats.incrementCounter(M_INDEXED, "type", "update", "source", event.getSource());
      update(event, writer, index);
    }

  }

  public TSDB tsdb() {
    return tsdb;
  }

  public String configId(final String suffix) {
    return ConfigUtils.configId(id, suffix);
  }

  private void doCreate(Event event, IndexWriter writer) throws Exception {
    stats.incrementCounter(M_INDEXED, "type", "new", "source", event.getSource());
    Document document = eventToDoc(event);
    writer.addDocument(document);
    if (event.getParentId() != null) {
      updateParent(event.getParentId(), event.getEventId());
    }
  }

  private void update(Event event, IndexWriter writer, String index) throws Exception {

    writer.commit(); // commit before doing a update query.

    IndexSearcher searcher = indexManager.getSearcher(index);
    final Term t;
    if (event.getAdditionalProps().containsKey(Event.UPDATE_ID)) {
      final String updateID = (String) event.getAdditionalProps().get(Event.UPDATE_ID);
      t = new Term(Event.UPDATE_ID.toLowerCase(), updateID.toLowerCase());
    } else {
      t = new Term(Fields.eventid.name(), event.getEventId());
    }

    Query query = new TermQuery(t);

    TopDocs search = searcher.search(query, 1);
    if (search.totalHits.value > 1) {
      stats.incrementCounter(M_UPDATES_SKIPPED, updateEventsTags);
      LOG.error("Cannot have 2 documents with the same EventID: {}", event);
      //throw new IllegalAccessException("Cannot have 2 documents with same eventID " + event);
      return;
    }

    if (search.totalHits.value == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Upserting event id {}", event.getEventId());
      }

      if (event.getAdditionalProps().containsKey(Event.UPDATE_ID)
          && event.getAdditionalProps().containsKey(Event.UPSERT_KEY)
          && Boolean.valueOf(event.getAdditionalProps().get(Event.UPSERT_KEY).toString())) {
        event.setEventId(EventWriter.generateEventID(event));

        doCreate(event, writer);
        return;
      } else {
        stats.incrementCounter(M_UPDATES_SKIPPED, updateEventsTags);
        LOG.error("No parents found for event {}", event);
        //throw new Exception("No parents found");
      }
    }
    ScoreDoc[] scoreDocs = search.scoreDocs;

    Document document = searcher.doc(scoreDocs[0].doc);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating event id " + event.getEventId());
    }
    doUpdate(t, writer, document, event);
  }


  public void doUpdate(Term term, IndexWriter writer, Document document, Event event)
      throws IOException {
    Event oldEvent = EventDocumentTranslater.docToEvent(document);

    // Lucene messes up updates and uses default analyzer for fields not specified as updates.
    // So go through old event and new event and add all fields.

    Document newDoc = eventToDoc(oldEvent);

    // Now do the updates.
    // Event id : Namespace + Source + Dimensions + timestamp - Don't update these

    addTextFieldIfPresent(Fields.title, event.getTitle(), newDoc, true);
    addTextFieldIfPresent(Fields.message, event.getMessage(), newDoc, true);
    addStringFieldIfPresent(Fields.priority, event.getPriority(), newDoc, true);
    addStringFieldIfPresent(Fields.userid, event.getUserId(), newDoc, true);
    addTimestamp(Fields.endtimestamp, event.getEndTimestamp(), newDoc, false, true);

    addAdditionalProps(event, newDoc, true);

    writer.updateDocument(term, newDoc);
    writer.commit();

  }

  private void updateParent(String parentId, String childId)
      throws Exception {
    String indexName = getIndexName(parentId);
    IndexWriter writer = indexManager.getIndexWriter(indexName);
    IndexSearcher searcher = indexManager.getSearcher(indexName);
    Term t = new Term(Fields.eventid.name(), parentId);
    Query query = new TermQuery(t);
    TopDocs search = searcher.search(query, 1);
    if (search.totalHits.value > 1) {
      throw new IllegalAccessException("Cannot have 2 documents with same eventID ");
    }
    ScoreDoc[] scoreDocs = search.scoreDocs;
    if (scoreDocs.length == 0) {
      throw new Exception("No parents found");
    }
    Document document = searcher.doc(scoreDocs[0].doc);
    document.add(new StringField(Fields.childid.name(), childId, Store.YES));

    writer.updateDocument(t, document);
  }

  public IndexManager indexManager() {
    return indexManager;
  }
}
