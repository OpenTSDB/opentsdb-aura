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

package net.opentsdb.aura.metrics.storage;

import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.common.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryPipelineContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.testng.AssertJUnit.*;
import static org.testng.AssertJUnit.assertEquals;

public class AggregatorFinishedTSTest {

  private static AerospikeBatchSourceFactory SOURCE_FACTORY;
  private static AerospikeClientPlugin AS_CLIENT;
  private static MockTSDB TSDB;

  private LTSAerospike asClient;
  private AerospikeBatchQueryNode.QR queryResult;
  private MetaTimeSeriesQueryResult metaResult;
  private MetaTimeSeriesQueryResult.GroupResult groupResult;
  private AerospikeBatchQueryNode queryNode;
  private QueryPipelineContext context;
  private QueryContext queryContext;


  @BeforeClass
  public static void beforeClass() {
    TSDB = new MockTSDB();
    TSDB.registry = new DefaultRegistry(TSDB);
    TSDB.registry.initialize(true);
    AS_CLIENT = mock(AerospikeClientPlugin.class);
    TSDB.registry.registerPlugin(AerospikeClientPlugin.class, null, AS_CLIENT);
    SOURCE_FACTORY = new AerospikeBatchSourceFactory();

    if (!TSDB.getConfig().hasProperty(AerospikeSourceFactory.SECONDS_IN_SEGMENT_KEY)) {
      TSDB.getConfig().register(AerospikeSourceFactory.SECONDS_IN_SEGMENT_KEY,
              3600 * 9,
              false,
              "Seconds in a segment.");
    }
    if (!TSDB.getConfig().hasProperty(AerospikeBatchSourceFactory.AS_BATCH_LIMIT_KEY)) {
      TSDB.getConfig().register(AerospikeBatchSourceFactory.AS_BATCH_LIMIT_KEY, 16, false, "UT");
    }
    if (!TSDB.getConfig().hasProperty(AerospikeBatchSourceFactory.AS_JOBS_PER_QUERY)) {
      TSDB.getConfig().register(AerospikeBatchSourceFactory.AS_JOBS_PER_QUERY, 4, false, "UT");
    }
  }

  @BeforeMethod
  public void before() {
    queryResult = mock(AerospikeBatchQueryNode.QR.class);
    metaResult = mock(MetaTimeSeriesQueryResult.class);
    groupResult = mock(MetaTimeSeriesQueryResult.GroupResult.class);
    asClient = mock(LTSAerospike.class);
    context = mock(QueryPipelineContext.class);
    queryContext = mock(QueryContext.class);
    //when(queryResult.metaResult()).thenReturn(metaResult);
    when(context.tsdb()).thenReturn(TSDB);
    when(context.queryContext()).thenReturn(queryContext);
    when(AS_CLIENT.asClient()).thenReturn(asClient);
    when(asClient.secondsInRecord()).thenReturn(3600 * 9);
    queryNode = mock(AerospikeBatchQueryNode.class);
    when(queryNode.queryResult()).thenReturn(queryResult);
    when(queryNode.metricString()).thenReturn("mymetric");
  }

  @Test
  public void timeSeriesId() throws Exception {
    when(queryNode.getStringForHash(24L)).thenReturn("web01");
    when(queryNode.getStringForHash(42L)).thenReturn("den");
    when(groupResult.tagHashes()).thenReturn(
            new MetaTimeSeriesQueryResult.GroupResult.TagHashes() {
              int idx = 0;
              @Override
              public int size() {
                return 2;
              }

              @Override
              public long next() {
                return idx++ > 0 ? 42 : 24;
              }
            });
    when(queryNode.gbKeys()).thenReturn(new String[] { "host", "dc" });
    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, null);

    TimeSeriesStringId id = (TimeSeriesStringId) finished.id();
    assertEquals("mymetric", id.metric());
    Map<String, String> tags = id.tags();
    assertEquals(2, tags.size());
    assertEquals("web01", tags.get("host"));
    assertEquals("den", tags.get("dc"));
    verify(queryNode, never()).onError(any(QueryDownstreamException.class));

    // cached
    id = (TimeSeriesStringId) finished.id();
    assertEquals("mymetric", id.metric());
    verify(queryNode, times(1)).gbKeys();
    tags = id.tags();
    assertEquals(2, tags.size());
    assertEquals("web01", tags.get("host"));
    assertEquals("den", tags.get("dc"));

    // just to satisfy coverage.
    assertNull(id.alias());
    assertNull(id.namespace());
    assertEquals("web01", id.getTagValue("host"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    assertNull(id.uniqueIds());
    assertEquals(0, id.hits());
    try {
      assertEquals(0, id.compareTo(null));
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    assertFalse(id.encoded());
    assertEquals(Const.TS_STRING_ID, id.type());
    try {
      id.buildHashCode();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }

  @Test
  public void timeSeriesIdGroupByException() throws Exception {
    when(queryNode.getStringForHash(24L)).thenReturn("web01");
    when(queryNode.getStringForHash(42L)).thenReturn("den");
    when(groupResult.tagHashes()).thenReturn(new MetaTimeSeriesQueryResult.GroupResult.TagHashes() {
      int idx = 0;
      @Override
      public int size() {
        return 2;
      }

      @Override
      public long next() {
        if (idx++ > 0) {
          throw new RuntimeException("BOO");
        }
        return 24;
      }
    });
    when(queryNode.gbKeys()).thenReturn(new String[] { "host", "dc"});

    AggregatorFinishedTS finished = new AggregatorFinishedTS(queryNode, groupResult, null);
    TimeSeriesStringId id = (TimeSeriesStringId) finished.id();
    assertEquals("mymetric", id.metric());
    Map<String, String> tags = id.tags();
    assertEquals(1, tags.size());
    verify(queryNode, times(1)).onError(any(QueryDownstreamException.class));
  }

}