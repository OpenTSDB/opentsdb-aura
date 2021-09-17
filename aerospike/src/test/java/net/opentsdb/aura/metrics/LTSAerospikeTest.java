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

import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.google.common.collect.Lists;
import net.opentsdb.aura.metrics.core.LongTermStorage;
import net.opentsdb.aura.metrics.core.RawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.gorilla.GorillaRawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.OnHeapGorillaSegment;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Verifications;
import net.opentsdb.stats.StatsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import static mockit.internal.expectations.ActiveInvocations.times;
import static org.junit.jupiter.api.Assertions.*;

public class LTSAerospikeTest {

  private static final String NAMESPACE = "Test";
  private static final String SET_NAME = "S";
  private static final String BIN_NAME = "b";

  private static final int BASE_TIME = 1614556800;
  private static final Random RND = new Random(System.currentTimeMillis());
  private static final int recordWidthInSeconds = 3600 * 2 * 3;
  private static final int secondsInASegment = 3600 * 2;

  @Injectable
  private ASCluster cluster;
  @Injectable
  private StatsCollector stats;
  @Injectable
  private ScheduledExecutorService scheduledExecutorService;
  @Injectable
  private RawTimeSeriesEncoder timeSeriesEncoder;
  @Mocked
  private OnHeapGorillaSegment gorillaSegment;
  @Mocked
  private GorillaRawTimeSeriesEncoder gorillaRawTimeSeriesEncoder;
  @Injectable
  private RecordIterator ri;
  @Mocked
  private ASSyncClient asClient;

  private LTSAerospike lts;

  @BeforeEach
  public void before() {
    lts = new LTSAerospike(cluster, NAMESPACE, recordWidthInSeconds, secondsInASegment, SET_NAME, BIN_NAME, stats, scheduledExecutorService);
  }

  @Test
  public void flushError() throws Exception {
    long hash = 42;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(BASE_TIME, key,8);

      timeSeriesEncoder.getSegmentTime();
      result = BASE_TIME;

      asClient.putInMap(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withInstanceOf(Value.class));
      result = ResultCode.FAIL_FORBIDDEN;
    }};

    assertFalse(lts.flush(hash, timeSeriesEncoder));
  }

  static Stream<Arguments> flushArgs() {
    return Stream.of(
            Arguments.of(RND.nextLong(), BASE_TIME, BASE_TIME, 0),
            Arguments.of(RND.nextLong(), BASE_TIME, BASE_TIME + secondsInASegment, 1),
            Arguments.of(RND.nextLong(), BASE_TIME, BASE_TIME + (secondsInASegment * 2), 2),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 3), BASE_TIME + (secondsInASegment * 3), 0),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 3), BASE_TIME + (secondsInASegment * 4), 1),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 3), BASE_TIME + (secondsInASegment * 5), 2),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 6), BASE_TIME + (secondsInASegment * 6), 0),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 6), BASE_TIME + (secondsInASegment * 7), 1),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 6), BASE_TIME + (secondsInASegment * 8), 2),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 9), BASE_TIME + (secondsInASegment * 9), 0),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 9), BASE_TIME + (secondsInASegment * 10), 1),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 9), BASE_TIME + (secondsInASegment * 11), 2),
            Arguments.of(RND.nextLong(), BASE_TIME + (secondsInASegment * 12), BASE_TIME + (secondsInASegment * 12), 0)
    );
  }

  @ParameterizedTest
  @MethodSource("flushArgs")
  public void flushAlignedSegments(long hash, int baseTs, int ts, int offset) throws Exception {
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(baseTs, key,8);

      timeSeriesEncoder.getSegmentTime();
      result = ts;

      asClient.putInMap(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(offset),
              withInstanceOf(Value.class));
      result = ResultCode.OK;
    }};

    assertTrue(lts.flush(hash, timeSeriesEncoder));
  }

  @Test
  public void readOneSegmentMissingKey() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(1));
      result = ri;

      ri.getResultCode();
      result = ResultCode.KEY_NOT_FOUND_ERROR;
    }};
    LTSAerospike lts = new LTSAerospike(cluster, NAMESPACE, recordWidthInSeconds, secondsInASegment, SET_NAME, BIN_NAME, stats, scheduledExecutorService);
    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (3600 * 2));
    assertFalse(records.hasNext());
  }

  @Test
  public void readOneSegmentAligned() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(1));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + secondsInASegment);
    assertTrue(records.hasNext());

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(ts),
              withEqual(new byte[] { 0 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readTwoSegmentsAligned() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(2));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (secondsInASegment * 2));
    assertTrue(records.hasNext());

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME),
              withEqual(new byte[] { 0 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + secondsInASegment),
              withEqual(new byte[] { 1 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readThreeSegmentsAligned() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (secondsInASegment * 3));
    assertTrue(records.hasNext());

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME),
              withEqual(new byte[] { 0 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + secondsInASegment),
              withEqual(new byte[] { 1 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readFourSegmentsAligned() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (secondsInASegment * 4));
    assertTrue(records.hasNext());

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME),
              withEqual(new byte[] { 0 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + secondsInASegment),
              withEqual(new byte[] { 1 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    // NEXT CALL
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts + (secondsInASegment * 3), key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(1));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 3 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    // next call to AS
    assertNotNull(records.next());
  }

  @Test
  public void readOneSegmentMiddle() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(1),
              withEqual(2));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash,
            BASE_TIME + secondsInASegment,
            BASE_TIME + (secondsInASegment * 2));
    assertTrue(records.hasNext());

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + secondsInASegment),
              withEqual(new byte[] { 1 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readOneSegmentEnd() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(2),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash,
            BASE_TIME + (secondsInASegment * 2),
            BASE_TIME + (secondsInASegment * 3));
    assertTrue(records.hasNext());

    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readTwoSegmentsCrossRecords() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(2),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash,
            BASE_TIME + (secondsInASegment * 2),
            BASE_TIME + (secondsInASegment * 4));

    // NEXT CALL
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts + (secondsInASegment * 3), key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(1));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 3 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readFullRecordMissingFirst() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      //payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (secondsInASegment * 3));

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + secondsInASegment),
              withEqual(new byte[] { 1 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readFullRecordMissingFirstAndSecond() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      //payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      //payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (secondsInASegment * 3));

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readFullRecordMissingSecond() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      //payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (secondsInASegment * 3));

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME),
              withEqual(new byte[] { 0 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readFullRecordMissingSecondAndThird() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      //payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));
      //payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (secondsInASegment * 3));

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME),
              withEqual(new byte[] { 0 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readFullRecordMissingThird() throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));
      //payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash, BASE_TIME, BASE_TIME + (secondsInASegment * 3));

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME),
              withEqual(new byte[] { 0 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + secondsInASegment),
              withEqual(new byte[] { 1 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readTwoRecordsMissingFirst(@Injectable RecordIterator ri2) throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.KEY_NOT_FOUND_ERROR;

      key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts + (secondsInASegment * 3), key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri2;

      ri2.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 3 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 4 }));

      ri2.advance();
      result = true;

      ri2.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash,
            BASE_TIME,
            BASE_TIME + (secondsInASegment * 6));
    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 3)),
              withEqual(new byte[] { 3 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 4)),
              withEqual(new byte[] { 4 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readThreeRecordsMissingFirstTwo(@Injectable RecordIterator ri2,
                                              @Injectable RecordIterator ri3) throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.KEY_NOT_FOUND_ERROR;

      key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts + (secondsInASegment * 3), key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri2;

      ri2.getResultCode();
      result = ResultCode.KEY_NOT_FOUND_ERROR;

      key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts + (secondsInASegment * 6), key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri3;

      ri3.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 6 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 7 }));

      ri3.advance();
      result = true;

      ri3.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash,
            BASE_TIME,
            BASE_TIME + (secondsInASegment * 9));
    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 6)),
              withEqual(new byte[] { 6 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 7)),
              withEqual(new byte[] { 7 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

  @Test
  public void readThreeRecordsMissingMiddle(@Injectable RecordIterator ri2,
                                            @Injectable RecordIterator ri3) throws Exception {
    long hash = RND.nextLong();
    int ts = BASE_TIME;
    new Expectations() {{
      byte[] key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts, key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri;

      ri.getResultCode();
      result = ResultCode.OK;

      List<Object> payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 0 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 1 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(2L, new byte[] { 2 }));

      ri.advance();
      result = true;

      ri.valueToObject();
      result = payload;

      key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts + (secondsInASegment * 3), key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri2;

      ri2.getResultCode();
      result = ResultCode.KEY_NOT_FOUND_ERROR;

      key = new byte[12];
      ByteArrays.putLong(hash, key, 0);
      ByteArrays.putInt(ts + (secondsInASegment * 6), key,8);

      asClient.mapRangeQuery(withEqual(key),
              withEqual(lts.getASSetHash()),
              withEqual(BIN_NAME.getBytes(StandardCharsets.UTF_8)),
              withEqual(0),
              withEqual(3));
      result = ri3;

      ri3.getResultCode();
      result = ResultCode.OK;

      payload = Lists.newArrayList();
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(0L, new byte[] { 6 }));
      payload.add( new AbstractMap.SimpleEntry<Long, byte[]>(1L, new byte[] { 7 }));

      ri3.advance();
      result = true;

      ri3.valueToObject();
      result = payload;
    }};

    LongTermStorage.Records records = lts.read(hash,
            BASE_TIME,
            BASE_TIME + (secondsInASegment * 9));
    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME),
              withEqual(new byte[] { 0 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 1)),
              withEqual(new byte[] { 1 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 2)),
              withEqual(new byte[] { 2 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 6)),
              withEqual(new byte[] { 6 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertTrue(records.hasNext());
    assertNotNull(records.next());
    new Verifications() {{
      new OnHeapGorillaSegment(withEqual(BASE_TIME + (secondsInASegment * 7)),
              withEqual(new byte[] { 7 }),
              withEqual(0),
              withEqual(1));
      times(1);
    }};

    assertFalse(records.hasNext());
  }

}