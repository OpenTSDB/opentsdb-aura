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

import net.opentsdb.events.Fields;
import net.opentsdb.events.view.Event;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class EventDocumentTranslater {

  public static Event docToEvent(Document doc) {
    Event event = new Event();
    event.setEventId(doc.get(Fields.eventid.name()));
    event.setSource(doc.get(Fields.source.name()));
    event.setNamespace(doc.get(Fields.namespace.name()));
    event.setPriority(doc.get(Fields.priority.name()));
    event.setTitle(doc.get(Fields.title.name()));
    event.setMessage(doc.get(Fields.message.name()));
    event.setUserId(doc.get(Fields.userid.name()));
    event.setOngoing(Boolean.valueOf(doc.get(Fields.ongoing.name())));
    event.setTimestamp(Long.valueOf(doc.get(Fields.starttimestamp.name())));
    String endTimestampStr = doc.get(Fields.endtimestamp.name());
    if (endTimestampStr != null) {
      event.setTimestamp(Long.valueOf(endTimestampStr));
    }

    event.setParentIds(Arrays.asList(doc.getValues(Fields.parentid.name())));
    event.setChildIds(Arrays.asList(doc.getValues(Fields.childid.name())));

    List<IndexableField> fields = doc.getFields();
    List<IndexableField> tagFields = fields.stream().filter(f -> f.name().contains("tag."))
        .collect(Collectors.toList());
    Map<String, String> tags = new HashMap<>();
    for (IndexableField f : tagFields) {
      tags.put(f.name().replaceFirst("tag.", ""), f.stringValue());
    }
    event.setDimensions(tags);

    List<IndexableField> propsFields = fields.stream().filter(f -> {
      boolean processed = true;
      for (Fields field : Fields.values()) {
        if (f.name().equalsIgnoreCase(field.name()) || f.name().startsWith("tag.")) {
          processed = false;
        }
      }
      return processed;
    }).collect(Collectors.toList());
    Map<String, Object> props = new HashMap<>();
    for (IndexableField f : propsFields) {
      props.put(f.name(), f.stringValue());
    }
    event.setAdditionalProps(props);
    return event;
  }


  public static Document eventToDoc(Event event) {
    Document document = new Document();

    addStringFieldIfPresent(Fields.eventid, event.getEventId(), document,false);

    addStringFieldIfPresent(Fields.namespace, event.getNamespace(), document, false);
    addStringFieldIfPresent(Fields.source, event.getSource(), document, false);
    addTextFieldIfPresent(Fields.title, event.getTitle(), document, false);
    addTextFieldIfPresent(Fields.message, event.getMessage(), document, false);

    addStringFieldIfPresent(Fields.userid, event.getUserId(), document, false);
    addStringFieldIfPresent(Fields.priority, event.getPriority(), document, false);

    addDimensions(event, document);
    addAdditionalProps(event, document, false);


    addTimestamp(Fields.starttimestamp, event.getTimestamp(), document, true, false);
    addTimestamp(Fields.endtimestamp, event.getEndTimestamp(), document, false, false);

    addStringFieldIfPresent(Fields.parentid, event.getParentId(), document, false);

    return document;
  }

  public static void addTimestamp(Fields field, long timestamp, Document document, boolean sort, boolean update) {
    if (timestamp != 0L) {
      if (update) document.removeFields(field.name());
      document.add(new LongPoint(field.name(), timestamp));  // store in seconds in db
      document.add(new StoredField(field.name(), timestamp));
      document.add(new NumericDocValuesField(field.name() + "-sort", timestamp));
      if (sort)
        document.add(new SortedDocValuesField(Fields.timestampMinute.name(),
            new BytesRef(String.valueOf(timestamp - (timestamp % 60)))));
    }
  }

  public static void addTextFieldIfPresent(Fields field, String text, Document doc, boolean update) {
    if (text != null) {
      if (update) doc.removeFields(field.name());
      doc.add(new TextField(field.name(), text, Store.YES));
    }
  }

  public static void addStringFieldIfPresent(Fields field, String text, Document doc, boolean update) {
    if(text != null) {
      if (update) doc.removeFields(field.name());
      doc.add(new StringField(field.name(), text.toLowerCase(), Store.YES));
    }
  }

  private static void addDimensions(Event event, Document document) {
    if (event.getDimensions() != null) {
      StringBuilder allTags = new StringBuilder();
      for (Entry<String, String> tags : event.getDimensions().entrySet()) {
        document.add(
            new StringField(("Tag." + tags.getKey()).toLowerCase(), tags.getValue().toLowerCase(),
                Store.NO));
        document.add(new StoredField(("Tag." + tags.getKey()).toLowerCase(), tags.getValue()));
        document.add(new SortedDocValuesField(("Tag." + tags.getKey()).toLowerCase(),
            new BytesRef(tags.getValue().toLowerCase())));
        allTags.append(tags.getKey());
        allTags.append(" ");
        allTags.append(tags.getValue());
        allTags.append(" ");
      }
      document.add(new TextField("tags", allTags.toString(),
          Store.NO)); // tags is a TextField, so no need to lowercase it explicitly
    }
  }

  public static void addAdditionalProps(Event event, Document document, boolean update) {
    if (event.getAdditionalProps() != null) {
      StringBuilder allProps = new StringBuilder();
      for (Entry<String, Object> tags : event.getAdditionalProps().entrySet()) {
        if (tags.getKey().equalsIgnoreCase(Fields.ongoing.name())) {
          if (update) document.removeFields(Fields.ongoing.name());
          document.add(
              new IntPoint(Fields.ongoing.name(), Integer.valueOf(tags.getValue().toString())));
          document.add(
              new StoredField(Fields.ongoing.name(), Integer.valueOf(tags.getValue().toString())));
        } else {
          if (update) {
            document.removeFields(tags.getKey().toLowerCase());
            document.removeFields(tags.getKey());
          }
          document.add(
              new StringField((tags.getKey()).toLowerCase(),
                  tags.getValue().toString().toLowerCase(),
                  Store.YES));
          document.add(new StoredField(tags.getKey(), tags.getValue().toString()));
        }
        allProps.append(tags.getKey());
        allProps.append(" ");
        allProps.append(tags.getValue());
        allProps.append(" ");
      }
      if (update) document.removeFields("props");
      document.add(new TextField("props", allProps.toString(), Store.NO));
    }
  }

}
