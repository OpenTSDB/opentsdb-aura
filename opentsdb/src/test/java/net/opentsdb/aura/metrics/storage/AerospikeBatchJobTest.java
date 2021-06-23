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

import com.beust.jcommander.internal.Lists;
import net.opentsdb.aura.metrics.AerospikeRecordMap;
import net.opentsdb.aura.metrics.BatchRecordIterator;
import net.opentsdb.aura.metrics.LTSAerospike;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.utils.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class AerospikeBatchJobTest {

  private static final int BASE_TS = 1617235200;
  private static final int SECONDS_IN_SEGMENT = 3600 * 2;
  private static final int SECONDS_IN_RECORD = 3600 * 9;

  private static AerospikeBatchJob JOB;
  private AerospikeBatchQueryNode node;
  private LTSAerospike client;
  private MetaTimeSeriesQueryResult metaResult;
  private CountDownLatch latch;
  private List<ASCaptures> asCaptures;
  private AerospikeRecordMap aerospikeRecordMap;

  @BeforeClass
  public static void beforeClass() throws Exception {
    JOB = new AerospikeBatchJob();
  }

  @Before
  public void before() throws Exception {
    node = mock(AerospikeBatchQueryNode.class);
    when(node.batchLimit()).thenReturn(16);
    latch = mock(CountDownLatch.class);

    client = mock(LTSAerospike.class);
    when(client.secondsInRecord()).thenReturn(SECONDS_IN_RECORD);
    when(node.secondsInRecord()).thenReturn(SECONDS_IN_RECORD);
    when(node.client()).thenReturn(client);

    asCaptures = Lists.newArrayList();
    aerospikeRecordMap = mock(AerospikeRecordMap.class);
  }

  @Test
  public void runAll1Group1Hash1Record() throws Exception {
    setup(BASE_TS, BASE_TS + client.secondsInRecord(), 1);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.endHashId = 1;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(1, makeKey(0, BASE_TS));
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void run1AllGroup1Hash3Records() throws Exception {
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 1);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.endHashId = 1;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(3, makeKey(0, BASE_TS),
            makeKey(0, BASE_TS + SECONDS_IN_RECORD),
            makeKey(0, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runAll2Groups1Hash1Record() throws Exception {
    setup(BASE_TS, BASE_TS + client.secondsInRecord(), 1, 1);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.endGroupId = 1;
    JOB.endHashId = 1;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(2, makeKey(0, BASE_TS),
            makeKey(1, BASE_TS));
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runAll2Groups1Hash3Records() throws Exception {
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 1, 1);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.endGroupId = 1;
    JOB.endHashId = 1;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(6, makeKey(0, BASE_TS),
            makeKey(0, BASE_TS + SECONDS_IN_RECORD),
            makeKey(0, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(1, BASE_TS),
            makeKey(1, BASE_TS+ SECONDS_IN_RECORD),
            makeKey(1, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runAll2Groups1Hash3RecordsBatchOf3() throws Exception {
    when(node.batchLimit()).thenReturn(2);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 1, 1);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.endGroupId = 1;
    JOB.endHashId = 1;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(2, makeKey(0, BASE_TS),
            makeKey(0, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(2, makeKey(0, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(1, BASE_TS));
    verify(node, times(2)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(2);
    capture.validate(2, makeKey(1, BASE_TS + SECONDS_IN_RECORD),
            makeKey(1, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(2)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void run2ndStartOfGroup() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 1;
    JOB.endGroupId = 1;
    JOB.startHashId = 0;
    JOB.endHashId = 3;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(3, BASE_TS),
            makeKey(3, BASE_TS + SECONDS_IN_RECORD),
            makeKey(3, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(4, BASE_TS),
            makeKey(4, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(4, makeKey(4, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(5, BASE_TS),
            makeKey(5, BASE_TS + SECONDS_IN_RECORD),
            makeKey(5, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(1)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void run2ndMiddleOfGroup() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 1;
    JOB.endGroupId = 1;
    JOB.startHashId = 2;
    JOB.endHashId = 4;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(5, BASE_TS),
            makeKey(5, BASE_TS + SECONDS_IN_RECORD),
            makeKey(5, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(6, BASE_TS),
            makeKey(6, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(1, makeKey(6, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(1)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void run2ndEndOfGroup() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 1;
    JOB.endGroupId = 1;
    JOB.startHashId = 4;
    JOB.endHashId = 6;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(7, BASE_TS),
            makeKey(7, BASE_TS + SECONDS_IN_RECORD),
            makeKey(7, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(8, BASE_TS),
            makeKey(8, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(1, makeKey(8, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(1)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void run2ndFullGroup() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 1;
    JOB.endGroupId = 1;
    JOB.startHashId = 0;
    JOB.endHashId = 6;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(3, BASE_TS),
            makeKey(3, BASE_TS + SECONDS_IN_RECORD),
            makeKey(3, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(4, BASE_TS),
            makeKey(4, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(5, makeKey(4, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(5, BASE_TS),
            makeKey(5, BASE_TS + SECONDS_IN_RECORD),
            makeKey(5, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(6, BASE_TS));
    verify(node, times(2)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(2);
    capture.validate(5, makeKey(6, BASE_TS + SECONDS_IN_RECORD),
            makeKey(6, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(7, BASE_TS),
            makeKey(7, BASE_TS + SECONDS_IN_RECORD),
            makeKey(7, BASE_TS+ (SECONDS_IN_RECORD * 2)));
    verify(node, times(3)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(3);
    capture.validate(3, makeKey(8, BASE_TS),
            makeKey(8, BASE_TS + SECONDS_IN_RECORD),
            makeKey(8, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(3)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runStartFullGroupPartNext() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 0;
    JOB.endGroupId = 1;
    JOB.startHashId = 0;
    JOB.endHashId = 1;
    JOB.run();

    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(0, BASE_TS),
            makeKey(0, BASE_TS + SECONDS_IN_RECORD),
            makeKey(0, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(1, BASE_TS),
            makeKey(1, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(5, makeKey(1, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(2, BASE_TS ),
            makeKey(2, BASE_TS + SECONDS_IN_RECORD),
            makeKey(2, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(3, BASE_TS));
    verify(node, times(2)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(2);
    capture.validate(2, makeKey(3, BASE_TS + SECONDS_IN_RECORD),
            makeKey(3, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(2)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runStartPartFirstGroupPartNext() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 0;
    JOB.endGroupId = 1;
    JOB.startHashId = 1;
    JOB.endHashId = 1;
    JOB.run();

    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(1, BASE_TS),
            makeKey(1, BASE_TS + SECONDS_IN_RECORD),
            makeKey(1, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(2, BASE_TS),
            makeKey(2, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(4, makeKey(2, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(3, BASE_TS ),
            makeKey(3, BASE_TS + SECONDS_IN_RECORD),
            makeKey(3, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(1)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runStart2FullGroups() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 0;
    JOB.endGroupId = 1;
    JOB.startHashId = 0;
    JOB.endHashId = 6;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(0, BASE_TS),
            makeKey(0, BASE_TS + SECONDS_IN_RECORD),
            makeKey(0, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(1, BASE_TS),
            makeKey(1, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(5, makeKey(1, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(2, BASE_TS),
            makeKey(2, BASE_TS + SECONDS_IN_RECORD),
            makeKey(2, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(3, BASE_TS));
    verify(node, times(2)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(2);
    capture.validate(5, makeKey(3, BASE_TS + SECONDS_IN_RECORD),
            makeKey(3, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(4, BASE_TS),
            makeKey(4, BASE_TS + SECONDS_IN_RECORD),
            makeKey(4, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(3)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(3);
    capture.validate(5, makeKey(5, BASE_TS),
            makeKey(5, BASE_TS + SECONDS_IN_RECORD),
            makeKey(5, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(6, BASE_TS),
            makeKey(6, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(4)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(4);
    capture.validate(5, makeKey(6, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(7, BASE_TS),
            makeKey(7, BASE_TS + SECONDS_IN_RECORD),
            makeKey(7, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(8, BASE_TS));
    verify(node, times(5)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(5);
    capture.validate(2, makeKey(8, BASE_TS + SECONDS_IN_RECORD),
            makeKey(8, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(5)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runEndFullGroupPartNext() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 1;
    JOB.endGroupId = 2;
    JOB.startHashId = 0;
    JOB.endHashId = 1;
    JOB.run();

    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(3, BASE_TS),
            makeKey(3, BASE_TS + SECONDS_IN_RECORD),
            makeKey(3, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(4, BASE_TS),
            makeKey(4, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(5, makeKey(4, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(5, BASE_TS ),
            makeKey(5, BASE_TS + SECONDS_IN_RECORD),
            makeKey(5, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(6, BASE_TS));
    verify(node, times(2)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(2);
    capture.validate(5, makeKey(6, BASE_TS + SECONDS_IN_RECORD),
            makeKey(6, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(7, BASE_TS),
            makeKey(7, BASE_TS + SECONDS_IN_RECORD),
            makeKey(7, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(3)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(3);
    capture.validate(5, makeKey(8, BASE_TS),
            makeKey(8, BASE_TS + SECONDS_IN_RECORD),
            makeKey(8, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(9, BASE_TS),
            makeKey(9, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(4)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(4);
    capture.validate(1, makeKey(9, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(4)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runEndPartGroupPartNext() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 1;
    JOB.endGroupId = 2;
    JOB.startHashId = 3;
    JOB.endHashId = 1;
    JOB.run();

    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(6, BASE_TS),
            makeKey(6, BASE_TS + SECONDS_IN_RECORD),
            makeKey(6, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(7, BASE_TS),
            makeKey(7, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(5, makeKey(7, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(8, BASE_TS ),
            makeKey(8, BASE_TS + SECONDS_IN_RECORD),
            makeKey(8, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(9, BASE_TS));
    verify(node, times(2)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(2);
    capture.validate(2, makeKey(9, BASE_TS + SECONDS_IN_RECORD),
            makeKey(9, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(2)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void runEnd2FullGroups() throws Exception {
    when(node.batchLimit()).thenReturn(5);
    setup(BASE_TS, BASE_TS + (client.secondsInRecord() * 3), 3, 6, 3);
    setupEmptyResult();
    JOB.reset(node, metaResult, latch);
    JOB.startGroupId = 1;
    JOB.endGroupId = 2;
    JOB.startHashId = 0;
    JOB.endHashId = 3;
    JOB.run();

    assertEquals(1, asCaptures.size());
    ASCaptures capture = asCaptures.get(0);
    capture.validate(5, makeKey(3, BASE_TS),
            makeKey(3, BASE_TS + SECONDS_IN_RECORD),
            makeKey(3, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(4, BASE_TS),
            makeKey(4, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(1)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(1);
    capture.validate(5, makeKey(4, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(5, BASE_TS),
            makeKey(5, BASE_TS + SECONDS_IN_RECORD),
            makeKey(5, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(6, BASE_TS));
    verify(node, times(2)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(2);
    capture.validate(5, makeKey(6, BASE_TS + SECONDS_IN_RECORD),
            makeKey(6, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(7, BASE_TS),
            makeKey(7, BASE_TS + SECONDS_IN_RECORD),
            makeKey(7, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(3)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(3);
    capture.validate(5, makeKey(8, BASE_TS),
            makeKey(8, BASE_TS + SECONDS_IN_RECORD),
            makeKey(8, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(9, BASE_TS),
            makeKey(9, BASE_TS + SECONDS_IN_RECORD));
    verify(node, times(4)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(4);
    capture.validate(5, makeKey(9, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(10, BASE_TS),
            makeKey(10, BASE_TS + SECONDS_IN_RECORD),
            makeKey(10, BASE_TS + (SECONDS_IN_RECORD * 2)),
            makeKey(11, BASE_TS));
    verify(node, times(5)).submit(JOB);

    JOB.run();
    capture = asCaptures.get(5);
    capture.validate(2, makeKey(11, BASE_TS + SECONDS_IN_RECORD),
            makeKey(11, BASE_TS + (SECONDS_IN_RECORD * 2)));
    verify(node, times(5)).submit(JOB);
    verify(latch, times(1)).countDown();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void processBatch() throws Exception {

  }

  void setup(int segmentStart, int segmentEnd, int... numHashesInGroup) {
    when(node.segmentsStart()).thenReturn(segmentStart);
    when(node.segmentsEnd()).thenReturn(segmentEnd);
    long hash = 0;
    DefaultMetaTimeSeriesQueryResult result = new DefaultMetaTimeSeriesQueryResult();
    for (int i = 0; i < numHashesInGroup.length; i++) {
      DefaultMetaTimeSeriesQueryResult.DefaultGroupResult gr = new DefaultMetaTimeSeriesQueryResult.DefaultGroupResult();
      gr.addTagHash(i);
      for (int x = 0; x < numHashesInGroup[i]; x++) {
        gr.addHash(hash++);
      }
      result.addGroupResult(gr);
    }
    metaResult = result;
  }

  void setupEmptyResult() {
    BatchRecordIterator bri = mock(BatchRecordIterator.class);
    when(bri.keyIndices()).thenReturn(new int[0]);
    when(client.batchRead(any(byte[][].class), anyInt())).thenAnswer(
            new Answer<BatchRecordIterator>() {
              @Override
              public BatchRecordIterator answer(InvocationOnMock invocation) throws Throwable {
                asCaptures.add(new ASCaptures(invocation));
                return bri;
              }
            });
  }

  byte[] makeKey(long hash, int timestamp) {
    byte[] key = new byte[12];
    Bytes.setLong(key, hash);
    Bytes.setInt(key, timestamp, 8);
    return key;
  }

  class ASCaptures {
    byte[][] keys;
    int keyIdx;

    public ASCaptures(InvocationOnMock invocation) {
      byte[][] sourceKeys = (byte[][]) invocation.getArguments()[0];
      keyIdx = (int) invocation.getArguments()[1];
      keys = new byte[sourceKeys.length][];
      System.arraycopy(sourceKeys, 0, keys, 0, keyIdx);
    }

    void validate(int keyIdx, byte[]... keys) {
      assertEquals(keyIdx, this.keyIdx);
      if (this.keys.length < keyIdx) {
        throw new AssertionError("Array length was " + this.keys.length
                + " expected " + keyIdx);
      }
      for (int i = 0; i < keys.length; i++) {
        if (Bytes.memcmp(keys[i], this.keys[i]) != 0) {
          throw new AssertionError("Keys differed at [" + i + "]. Expected\n"
                  + Arrays.toString(keys[i]) + " got\n" + Arrays.toString(this.keys[i]));
        }
      }
    }
  }

}