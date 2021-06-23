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

package net.opentsdb.aura.metrics.meta;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MergedMetaTimeSeriesQueryResultTest {

  @Test
  public void addWithDuplicates() throws Exception {
    DefaultMetaTimeSeriesQueryResult.DefaultDictionary dict1 = new DefaultMetaTimeSeriesQueryResult.DefaultDictionary();
    dict1.put(-1L, "web01".getBytes(StandardCharsets.UTF_8));
    dict1.put(2L, "web02".getBytes(StandardCharsets.UTF_8));

    DefaultMetaTimeSeriesQueryResult dr1 = new DefaultMetaTimeSeriesQueryResult();
    dr1.setDictionary(dict1);
    DefaultMetaTimeSeriesQueryResult.DefaultGroupResult gr = new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult();
    gr.addHash(42L);
    gr.addHash(24L);
    gr.addHash(1L);
    gr.addTagHash(-1L);
    dr1.addGroupResult(gr);

    gr = new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult();
    gr.addHash(2L);
    gr.addTagHash(2L);
    dr1.addGroupResult(gr);

    assertEquals(2, dr1.numGroups());
    assertEquals(4, dr1.totalHashes());

    DefaultMetaTimeSeriesQueryResult.DefaultDictionary dict2 = new DefaultMetaTimeSeriesQueryResult.DefaultDictionary();
    dict2.put(-1, "web01".getBytes(StandardCharsets.UTF_8));
    dict2.put(-3L, "web03".getBytes(StandardCharsets.UTF_8));
    DefaultMetaTimeSeriesQueryResult dr2 = new DefaultMetaTimeSeriesQueryResult();
    dr2.setDictionary(dict2);
    gr = new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult();
    gr.addHash(13L);
    gr.addHash(42L);
    gr.addTagHash(-1L);
    dr2.addGroupResult(gr);

    gr = new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult();
    gr.addHash(1024L);
    gr.addHash(1025L);
    gr.addTagHash(-3L);
    dr2.addGroupResult(gr);

    assertEquals(2, dr2.numGroups());
    assertEquals(4, dr2.totalHashes());

    DefaultMetaTimeSeriesQueryResult.DefaultDictionary dict3 = new DefaultMetaTimeSeriesQueryResult.DefaultDictionary();
    dict3.put(-1, "web01".getBytes(StandardCharsets.UTF_8));
    DefaultMetaTimeSeriesQueryResult dr3 = new DefaultMetaTimeSeriesQueryResult();
    dr3.setDictionary(dict3);
    gr = new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult();
    gr.addHash(13L);
    gr.addTagHash(-1L);
    dr3.addGroupResult(gr);

    assertEquals(1, dr3.numGroups());
    assertEquals(1, dr3.totalHashes());

    MergedMetaTimeSeriesQueryResult merged = new MergedMetaTimeSeriesQueryResult();
    merged.add(dr1);
    assertEquals(2, merged.numGroups());
    assertEquals(4, merged.totalHashes());

    merged.add(dr2);
    assertEquals(3, merged.numGroups());
    assertEquals(7, merged.totalHashes());

    merged.add(dr3);
    assertEquals(3, merged.numGroups());
    assertEquals(7, merged.totalHashes());
  }

}