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
package net.opentsdb.events.view;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


@SuppressWarnings("serial")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Event implements KryoSerializable {

  private String namespace = "";

  @JsonProperty("source")
  private String source = null;

  @JsonProperty("title")
  private String title = null;

  @JsonProperty("message")
  private String message = null;

  @JsonProperty("priority")
  private String priority = null;

  private long timestamp = 0L;

  private long endTimestamp = 0L;

  @JsonProperty("userId")
  private String userId = null;

  @JsonProperty("ongoing")
  private boolean ongoing;

  @JsonProperty("eventId")
  private String eventId = null;

  @JsonProperty("parentId")
  private String parentId = null;


  @JsonProperty("childId")
  private String childId = null;

  private Map<String, String> dimensions = new HashMap<>();

  private Map<String, Object> additionalProps = new HashMap<>();

  private boolean isUpdate;


  private List<String> parentIds;

  private List<String> childIds;

  public static final String UPDATE_ID = "__update_id";
  public static final String UPSERT_KEY = "__upsert";

  public Event() {
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public Map<String, String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(Map<String, String> dimensions) {
    this.dimensions = dimensions;
  }

  @JsonProperty("title")
  public String getTitle() {
    return title;
  }

  @JsonProperty("title")
  public void setTitle(String title) {
    this.title = title;
  }

  @JsonProperty("message")
  public String getMessage() {
    return message;
  }

  @JsonProperty("message")
  public void setMessage(String message) {
    this.message = message;
  }

  public String getPriority() {
    return priority;
  }

  public void setPriority(String priority) {
    this.priority = priority;
  }

  public String getEventId() {
    return eventId;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }


  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public void setEndTimestamp(long endTimestamp) {
    this.endTimestamp = endTimestamp;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public boolean isOngoing() {
    return ongoing;
  }

  public void setOngoing(boolean ongoing) {
    this.ongoing = ongoing;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getParentId() {
    return parentId;
  }

  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  public String getChildId() {
    return childId;
  }

  public void setChildId(String childId) {
    this.childId = childId;
  }

  public Map<String, Object> getAdditionalProps() {
    return additionalProps;
  }

  public void setAdditionalProps(Map<String, Object> additionalProps) {
    this.additionalProps = additionalProps;
  }

  public boolean isUpdate() {
    return isUpdate;
  }

  public void setUpdate(boolean update) {
    isUpdate = update;
  }


  public void setParentIds(List<String> parentIds) {
    this.parentIds = parentIds;
  }

  public void setChildIds(List<String> childIds) {
    this.childIds = childIds;
  }

  public List<String> getParentIds() {
    return parentIds;
  }

  public List<String> getChildIds() {
    return childIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Event event = (Event) o;
    return Objects.equals(namespace, event.namespace)
        && Objects.equals(timestamp, event.timestamp)
        && Objects.equals(endTimestamp, event.endTimestamp)
        && Objects.equals(dimensions, event.dimensions)
        && Objects.equals(title, event.title)
        && Objects.equals(message, event.message)
        && Objects.equals(userId, event.userId)
        && Objects.equals(priority, event.priority)
        && Objects.equals(eventId, event.eventId)
        && Objects.equals(additionalProps, event.additionalProps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, timestamp, endTimestamp, dimensions, title,
        message, userId, ongoing, priority, eventId, additionalProps);
  }


  @Override
  public void write(Kryo kryo, Output output) {
    output.writeString(namespace);
    output.writeString(source);
    output.writeString(title);
    output.writeString(message);
    output.writeString(priority);
    output.writeLong(timestamp);
    output.writeLong(endTimestamp);
    output.writeString(userId);
    output.writeBoolean(ongoing);
    output.writeString(eventId);
    output.writeString(parentId); // no childid needed
    output.writeBoolean(isUpdate);

    output.writeShort(dimensions.size());
    for (Map.Entry<String, String> tags : dimensions.entrySet()) {
      output.writeString(tags.getKey());
      output.writeString(tags.getValue());
    }

    output.writeShort(additionalProps.size());
    for (Map.Entry<String, Object> props : additionalProps.entrySet()) {
      output.writeString(props.getKey());
      output.writeString(props.getValue().toString());
    }
  }


  @Override
  public void read(Kryo kryo, Input input) {

    namespace = input.readString();
    source = input.readString();
    title = input.readString();
    message = input.readString();
    priority = input.readString();
    timestamp = input.readLong();
    endTimestamp = input.readLong();
    userId = input.readString();
    ongoing = input.readBoolean();
    eventId = input.readString();
    parentId = input.readString(); // no childid needed
    isUpdate = input.readBoolean();

    int tagsSize = input.readShort();
    Map<String, String> tags = new HashMap<>(tagsSize);
    for (int i = 0; i < tagsSize; i++) {
      tags.put(input.readString(), input.readString());
    }
    dimensions.putAll(tags);

    int propsSize = input.readShort();
    for (int i = 0; i < propsSize; i++) {
      additionalProps.put(input.readString(), input.readString());
    }
  }


  @Override
  public String toString() {
    return "Event{" +
        "namespace='" + namespace + '\'' +
        ", source='" + source + '\'' +
        ", title='" + title + '\'' +
        ", message='" + message + '\'' +
        ", priority='" + priority + '\'' +
        ", timestamp=" + timestamp +
        ", endTimestamp=" + endTimestamp +
        ", userId='" + userId + '\'' +
        ", ongoing=" + ongoing +
        ", eventId='" + eventId + '\'' +
        ", parentId='" + parentId + '\'' +
        ", childId='" + childId + '\'' +
        ", dimensions=" + dimensions +
        ", additionalProps=" + additionalProps +
        ", isUpdate=" + isUpdate +
        ", parentIds=" + parentIds +
        ", childIds=" + childIds +
        '}';
  }
}

