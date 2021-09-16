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

package net.opentsdb.events;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.events.view.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class EventWriter implements Indexer, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(EventWriter.class);
  public static DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<>(10000);
  private static LongHashFunction hash = LongHashFunction.xx();

  public static void queue(Event event) throws InterruptedException {
    queue.put(event);
  }

  public static String getIndexName(String eventId) {
    return eventId.split("_")[0];
  }

  public static String generateEventID(Event event) {
    long eventId = hash.hashBytes((event.getNamespace() +
        event.getSource() +
        event.getDimensions().toString() +
        event.getTimestamp() * 1000L).getBytes());
    event.setEventId(String.valueOf(eventId));

    String indexName = getIndexName(event);

    return indexName + "_" + eventId;
  }

  public static String getIndexName(Event event) {
    return getIndexName(event.getTimestamp());
  }

  /**
   * @param timestamp in epoch seconds
   * @return the index name based on the timestamp
   */
  public static String getIndexName(long timestamp) {
    LocalDateTime date =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp * 1000L), ZoneId.of("UTC"));

    String indexDate = df.format(date);
    return indexDate;
  }

  public void run() {
    while (true) {
      try {
        Event event = queue.take();
        index(event);
      } catch (Throwable th) {
        LOG.error("Error: got an exception", th);
      }
    }
  }

}
