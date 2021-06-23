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

import net.opentsdb.aura.metrics.core.data.ResultantPointerArray;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuraMetricsTimeSeriesListTest {

  private AuraMetricsQueryResult result;
  private QueryNode node;
  private QueryPipelineContext context = mock(QueryPipelineContext.class);
  private TSDB tsdb = mock(TSDB.class);
  private Registry registry = mock(Registry.class);

  @Before
  public void before() throws Exception {
    result = mock(AuraMetricsQueryResult.class);
    node = mock(QueryNode.class);
    when(result.source()).thenReturn(node);
    when(node.pipelineContext()).thenReturn(context);
    when(context.tsdb()).thenReturn(tsdb);
    when(tsdb.getRegistry()).thenReturn(registry);
  }

  @Test
  public void ctor() throws Exception {
    long[] pointers = new long[4];
    for (int i = 0; i < 4; i++) {
      ResultantPointerArray rpa = new ResultantPointerArray(1);
      rpa.set(0, i, i + 1, 42);
      rpa.setTSCount(1);
      pointers[i] = rpa.getAddress();
    }
    AuraMetricsTimeSeriesList list = new AuraMetricsTimeSeriesList(result, pointers, 1, 1);
    assertEquals(4, list.counts.length);
    assertEquals(4, list.pointers.length);
    for (int i = 0; i < 4; i++) {
      ResultantPointerArray rpa = list.pointers[i];
      assertEquals(1, list.counts[0]);
      assertEquals(1, rpa.getCapacity());
      assertEquals(i + 1, rpa.getTagAddress(0));
      assertEquals(i, rpa.getTSAddress(0));
      rpa.free();
    }
    assertEquals(4, list.size());
  }

  @Test
  public void ctorOneEmpty() throws Exception {
    long[] pointers = new long[4];
    for (int i = 0; i < 4; i++) {
      if (i == 2) {
        continue;
      }
      ResultantPointerArray rpa = new ResultantPointerArray(1);
      rpa.set(0, i, i + 1, 42);
      rpa.setTSCount(1);
      pointers[i] = rpa.getAddress();
    }
    AuraMetricsTimeSeriesList list = new AuraMetricsTimeSeriesList(result, pointers, 1, 1);
    assertEquals(4, list.counts.length);
    assertEquals(4, list.pointers.length);
    for (int i = 0; i < 4; i++) {
      ResultantPointerArray rpa = list.pointers[i];
      assertEquals(1, list.counts[0]);
      if (i == 2) {
        assertEquals(0, rpa.getCapacity());
      } else {
        assertEquals(1, rpa.getCapacity());
        assertEquals(i + 1, rpa.getTagAddress(0));
        assertEquals(i, rpa.getTSAddress(0));
        rpa.free();
      }
    }
    assertEquals(3, list.size());
  }

  @Test
  public void iterateAndGetAllPresent() throws Exception {
    long[] pointers = new long[3];
    ResultantPointerArray rpa = new ResultantPointerArray(3);
    int v = 1;
    for (int i = 0; i < 3; i++) {
      rpa.set(i, v, v + 1, 2);
      v++;
    }
    rpa.setTSCount(3);
    pointers[0] = rpa.getAddress();

    rpa = new ResultantPointerArray(2);
    rpa.set(0, v, v + 1, 1);
    v++;
    rpa.set(1, v, v + 1, 1);
    v++;
    rpa.setTSCount(2);
    pointers[1] = rpa.getAddress();

    rpa = new ResultantPointerArray(2);
    rpa.set(0, v, v + 1, 1);
    v++;
    rpa.set(1, v, v + 1, 1);
    v++;
    rpa.setTSCount(2);
    pointers[2] = rpa.getAddress();

    AuraMetricsTimeSeriesList list = new AuraMetricsTimeSeriesList(result, pointers, 1, 1);
    assertEquals(3, list.counts.length);
    assertEquals(3, list.pointers.length);
    assertEquals(7, list.size());

    v = 1;
    for (int i = 0; i < list.size(); i++) {
      AuraMetricsTimeSeries amts = (AuraMetricsTimeSeries) list.get(i);
      assertEquals(v, amts.tsPointer);
      assertEquals(v + 1, amts.tagPointer);
      v++;
    }

    v = 1;
    for (final TimeSeries ts : list) {
      AuraMetricsTimeSeries amts = (AuraMetricsTimeSeries) ts;
      assertEquals(v, amts.tsPointer);
      assertEquals(v + 1, amts.tagPointer);
      v++;
    }

    list.close();
  }

  @Test
  public void iterateAndGetEmptyMiddle() throws Exception {
    long[] pointers = new long[3];
    ResultantPointerArray rpa = new ResultantPointerArray(3);
    int v = 1;
    for (int i = 0; i < 3; i++) {
      rpa.set(i, v, v + 1, 2);
      v++;
    }
    rpa.setTSCount(3);
    pointers[0] = rpa.getAddress();

    pointers[1] = 0; // yes, already 0 but now you KNOW it's 0.

    rpa = new ResultantPointerArray(2);
    rpa.set(0, v, v + 1, 1);
    v++;
    rpa.set(1, v, v + 1, 1);
    v++;
    rpa.setTSCount(2);
    pointers[2] = rpa.getAddress();

    AuraMetricsTimeSeriesList list = new AuraMetricsTimeSeriesList(result, pointers, 1, 1);
    assertEquals(3, list.counts.length);
    assertEquals(3, list.pointers.length);
    assertEquals(5, list.size());

    v = 1;
    for (int i = 0; i < list.size(); i++) {
      AuraMetricsTimeSeries amts = (AuraMetricsTimeSeries) list.get(i);
      assertEquals(v, amts.tsPointer);
      assertEquals(v + 1, amts.tagPointer);
      v++;
    }

    v = 1;
    for (final TimeSeries ts : list) {
      AuraMetricsTimeSeries amts = (AuraMetricsTimeSeries) ts;
      assertEquals(v, amts.tsPointer);
      assertEquals(v + 1, amts.tagPointer);
      v++;
    }
  }

  @Test
  public void iterateAndGetEmptyFirst() throws Exception {
    long[] pointers = new long[3];
    int v = 4;
    ResultantPointerArray rpa = new ResultantPointerArray(2);
    rpa.set(0, v, v + 1, 1);
    v++;
    rpa.set(1, v, v + 1, 1);
    v++;
    rpa.setTSCount(2);
    pointers[1] = rpa.getAddress();

    rpa = new ResultantPointerArray(2);
    rpa.set(0, v, v + 1, 1);
    v++;
    rpa.set(1, v, v + 1, 1);
    v++;
    rpa.setTSCount(2);
    pointers[2] = rpa.getAddress();

    AuraMetricsTimeSeriesList list = new AuraMetricsTimeSeriesList(result, pointers, 1, 1);
    assertEquals(3, list.counts.length);
    assertEquals(3, list.pointers.length);
    assertEquals(4, list.size());

    v = 4;
    for (int i = 0; i < list.size(); i++) {
      AuraMetricsTimeSeries amts = (AuraMetricsTimeSeries) list.get(i);
      assertEquals(v, amts.tsPointer);
      assertEquals(v + 1, amts.tagPointer);
      v++;
    }

    v = 4;
    for (final TimeSeries ts : list) {
      AuraMetricsTimeSeries amts = (AuraMetricsTimeSeries) ts;
      assertEquals(v, amts.tsPointer);
      assertEquals(v + 1, amts.tagPointer);
      v++;
    }

    list.close();
  }

  @Test
  public void iterateAndGetEmptyEnd() throws Exception {
    long[] pointers = new long[3];
    ResultantPointerArray rpa = new ResultantPointerArray(3);
    int v = 1;
    for (int i = 0; i < 3; i++) {
      rpa.set(i, v, v + 1, 2);
      v++;
    }
    rpa.setTSCount(3);
    pointers[0] = rpa.getAddress();

    rpa = new ResultantPointerArray(2);
    rpa.set(0, v, v + 1, 1);
    v++;
    rpa.set(1, v, v + 1, 1);
    v++;
    rpa.setTSCount(2);
    pointers[1] = rpa.getAddress();

    AuraMetricsTimeSeriesList list = new AuraMetricsTimeSeriesList(result, pointers, 1, 1);
    assertEquals(3, list.counts.length);
    assertEquals(3, list.pointers.length);
    assertEquals(5, list.size());

    v = 1;
    for (int i = 0; i < list.size(); i++) {
      AuraMetricsTimeSeries amts = (AuraMetricsTimeSeries) list.get(i);
      assertEquals(v, amts.tsPointer);
      assertEquals(v + 1, amts.tagPointer);
      v++;
    }

    v = 1;
    for (final TimeSeries ts : list) {
      AuraMetricsTimeSeries amts = (AuraMetricsTimeSeries) ts;
      assertEquals(v, amts.tsPointer);
      assertEquals(v + 1, amts.tagPointer);
      v++;
    }

    list.close();
  }

  @Test
  public void iterateAndGetEmpty() throws Exception {
    long[] pointers = new long[3];
    AuraMetricsTimeSeriesList list = new AuraMetricsTimeSeriesList(result, pointers, 1, 1);
    assertEquals(3, list.counts.length);
    assertEquals(3, list.pointers.length);
    assertEquals(0, list.size());

    try {
      list.get(0);
      fail("Expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException e) { }

    Iterator<TimeSeries> iterator = list.iterator();
    assertFalse(iterator.hasNext());
  }
}
