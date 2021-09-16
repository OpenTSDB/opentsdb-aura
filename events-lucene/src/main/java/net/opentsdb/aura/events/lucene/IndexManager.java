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
import net.opentsdb.configuration.Configuration;
import net.opentsdb.events.Fields;
import net.opentsdb.stats.StatsCollector;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class IndexManager extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(IndexManager.class);

  public static final String M_INDICES_COMMITTED = "events.indices.committed";
  public static final String M_INDICES = "events.indices.total";

  private volatile Map<String, IndexWriter> writers = new ConcurrentHashMap<>();
  private volatile Map<String, DirectoryReader> readers = new ConcurrentHashMap<>();
  private final LuceneWriter writer;
  private final String dataPath;
  private final StatsCollector stats;
  private final int commitFrequency;

  protected IndexManager(final LuceneWriter writer) {
    this.writer = writer;
    this.setName("Lucene commit thread");
    this.setDaemon(true);

    final Configuration config = writer.tsdb().getConfig();
    String temp = config.getString(writer.configId(ConfigUtils.STORAGE_DIR_KEY));
    if (temp.endsWith("/")) {
      dataPath = temp;
    } else {
      dataPath = temp + "/";
    }
    commitFrequency = config.getInt(writer.configId(ConfigUtils.COMMIT_FREQUENCY_KEY));
    stats = writer.tsdb().getStatsCollector();
  }

  public void shutdown() {
    for (IndexWriter writer : writers.values()) {
      try {
        writer.commit();
        writer.close();
      } catch (IOException e) {
        LOG.error("Failed to close Lucene writer: " + writer);
      }
    }
    for (DirectoryReader reader : readers.values()) {
      try {
        reader.close();
      } catch (IOException e) {
        LOG.error("Failed to close Lucene reader: " + reader);
      }
    }
    this.interrupt();
  }

  @Override
  public void run() {
    while (true) {
      try {
        int count = 0;
        for (Entry<String, IndexWriter> entry : writers.entrySet()) {
          IndexWriter writer = entry.getValue();
          writer.commit();
          writer.flush();
          count++;
          LOG.debug("Committed records into index: " + entry.getKey() + " " + entry.getValue()
                  .getDocStats().numDocs);
        }

        stats.setGauge(M_INDICES_COMMITTED, count);
        long open = Files.find(
                Paths.get(dataPath),
                1,
                (path, attributes) -> attributes.isDirectory()
        ).count();
        stats.setGauge(M_INDICES, open);
        Thread.sleep(commitFrequency);
      } catch (InterruptedException e) {
        return;
      } catch (Throwable throwable) {
        LOG.info("Error committing to Lucene index" + throwable);
      }
    }
  }

  public IndexSearcher getSearcher(String index) throws IOException {
    DirectoryReader reader;
    if (readers.get(index) == null) {
      Directory dir = FSDirectory
          .open(Paths.get(dataPath + index));
      reader = DirectoryReader.open(dir);
      readers.put(index, reader);
    } else {
      reader = readers.get(index);
    }
    if (!reader.isCurrent()) {
      reader = DirectoryReader.openIfChanged(reader);
    }
    IndexSearcher searcher = new IndexSearcher(reader);
    return searcher;
  }

  public IndexWriter getIndexWriter(String index) throws IOException {
    if (writers.get(index) == null) {
      synchronized (this) {
        if (writers.get(index) == null) {
          createIndex(index);
        }
      }
    }
    return writers.get(index);
  }

  public void createIndex(String index) throws IOException {
    LOG.info("Create a new index with name " + index);
    File dir = new File(dataPath + index);
    dir.mkdirs();
    createIndexWriter(index);
  }

  private IndexWriter createIndexWriter(String index) throws IOException {
    Map<String, Analyzer> analyzerPerField = new HashMap<>();
    analyzerPerField.put(Fields.title.name(), LuceneQueryBuilder.buildAnalyzer());
    analyzerPerField.put(Fields.message.name(), LuceneQueryBuilder.buildAnalyzer());
    analyzerPerField.put("props", LuceneQueryBuilder.buildAnalyzer());
    analyzerPerField.put("tags", LuceneQueryBuilder.buildAnalyzer());
    PerFieldAnalyzerWrapper aWrapper =
        new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), analyzerPerField);

    Path path = FileSystems.getDefault().getPath(dataPath + index);
    Directory directory = FSDirectory.open(path);
    IndexWriterConfig config = new IndexWriterConfig(aWrapper);
    config.setOpenMode(OpenMode.CREATE_OR_APPEND);
    IndexWriter writer = new IndexWriter(directory, config);
    writers.put(index, writer);
    return writer;
  }

  public Map<String, IndexWriter> getAllWriters() {
    return writers;
  }

  public Map<String, DirectoryReader> getAllReaders() {
    return readers;
  }

  public StatsCollector statsCollector() {
    return stats;
  }

  public String dataPath() {
    return dataPath;
  }
}
