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

package net.opentsdb.aura.metrics;

import net.opentsdb.aura.metrics.core.Util;
import net.opentsdb.aura.metrics.core.XxHash;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.hashing.HashFunction;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestUtil {

  private static HashFunction hashFunction = new XxHash();

  public static LowLevelMetricData.HashedLowLevelMetricData buildEvent(
      String metric, Map<String, String> tags, int[] times, double[] values) {

    int tagCount = tags.size();
    byte[] metricBytes = metric.getBytes();
    byte[] tagBytes = Util.serializeTags(tags);

    final long metricHash = hashFunction.hash(metricBytes);
    final long tagHash = hashFunction.hash(tagBytes);
    final long seriesHash = hashFunction.update(metricHash, tagHash);

    AtomicInteger index = new AtomicInteger(-1);

    LowLevelMetricData.HashedLowLevelMetricData event =
        new LowLevelMetricData.HashedLowLevelMetricData() {
          int tagKeyStart = 0;
          int tagValueStart = 0;
          int tagValueEnd = 0;

          @Override
          public long metricHash() {
            return metricHash;
          }

          @Override
          public StringFormat metricFormat() {
            return StringFormat.UTF8_STRING;
          }

          @Override
          public int metricStart() {
            return 0;
          }

          @Override
          public int metricLength() {
            return metricBytes.length;
          }

          @Override
          public byte[] metricBuffer() {
            return metricBytes;
          }

          @Override
          public ValueFormat valueFormat() {
            return ValueFormat.DOUBLE;
          }

          @Override
          public long longValue() {
            return 0;
          }

          @Override
          public float floatValue() {
            return 0;
          }

          @Override
          public double doubleValue() {
            return values[index.get()];
          }

          @Override
          public long timeSeriesHash() {
            return seriesHash;
          }

          @Override
          public long tagsSetHash() {
            return tagHash;
          }

          @Override
          public long tagPairHash() {
            return 0;
          }

          @Override
          public long tagKeyHash() {
            return 0;
          }

          @Override
          public long tagValueHash() {
            return 0;
          }

          @Override
          public void close() {
          }

          @Override
          public boolean advance() {
            if (index.get() >= values.length - 1) {
              return false;
            } else {
              index.getAndIncrement();
              return true;
            }
          }

          @Override
          public boolean hasParsingError() {
            return false;
          }

          @Override
          public String parsingError() {
            return null;
          }

          @Override
          public TimeStamp timestamp() {
            return new SecondTimeStamp(times[index.get()]);
          }

          @Override
          public byte[] tagsBuffer() {
            return tagBytes;
          }

          @Override
          public int tagBufferStart() {
            return 0;
          }

          @Override
          public int tagBufferLength() {
            return tagBytes.length;
          }

          @Override
          public StringFormat tagsFormat() {
            return StringFormat.UTF8_STRING;
          }

          @Override
          public int tagSetCount() {
            return tagCount;
          }

          @Override
          public byte tagDelimiter() {
            return 0;
          }

          @Override
          public boolean advanceTagPair() {
            if (tagValueEnd + 1 >= tagBytes.length) {
              return false;
            }
            tagKeyStart = tagValueEnd > 0 ? tagValueEnd + 1 : 0;
            tagValueStart = findTagEnd(tagKeyStart) + 1;
            tagValueEnd = findTagEnd(tagValueStart);
            return true;
          }

          @Override
          public int tagKeyStart() {
            return tagKeyStart;
          }

          @Override
          public int tagKeyLength() {
            return tagValueStart - 1 - tagKeyStart;
          }

          @Override
          public int tagValueStart() {
            return tagValueStart;
          }

          @Override
          public int tagValueLength() {
            return tagValueEnd - tagValueStart;
          }

          int findTagEnd(int start) {
            while (start < tagBytes.length) {
              if (tagBytes[start] == 0) {
                return start;
              }
              start++;
            }
            return start;
          }
        };
    return event;
  }
}
