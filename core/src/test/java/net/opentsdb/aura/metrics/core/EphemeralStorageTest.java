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

package net.opentsdb.aura.metrics.core;

import net.opentsdb.aura.metrics.meta.MetaDataStoreFactory;
import net.opentsdb.aura.metrics.meta.NewDocStore;
import io.ultrabrew.metrics.MetricRegistry;
import mockit.Expectations;
import mockit.Injectable;
import net.opentsdb.hashing.HashFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EphemeralStorageTest {

  @Injectable private MemoryInfoReader memoryInfoReader;
  @Injectable private MetricRegistry metricRegistry;
  @Injectable private HashFunction hashFunction;
  @Injectable private TimeSeriesEncoderFactory encoderFactory;
  @Injectable private MetaDataStoreFactory dataStoreFactory;
  @Injectable private ScheduledExecutorService scheduledExecutorService;

  private EphemeralStorage storage;
  private TimeseriesStorageContext context;

  private ShardConfig config = new ShardConfig();

  private int now = (int) (System.currentTimeMillis() / 1000);

  private int segmentTime = (int) (now - (now % TimeUnit.HOURS.toSeconds(config.segmentSizeHour)));

  int currentTimeHour = Util.getCurrentWallClockHour();
  LocalDateTime segmentDateTime =
      Instant.ofEpochSecond(segmentTime).atZone(ZoneId.systemDefault()).toLocalDateTime();

  @BeforeEach
  void beforeEach() {
    new Expectations() {
      {
        dataStoreFactory.create();
        result = new NewDocStore(null);
      }
    };
    config.segmentSizeHour = 1;
    segmentTime = (int) (now - (now % TimeUnit.HOURS.toSeconds(config.segmentSizeHour)));

    LocalDateTime segmentDateTime =
        Instant.ofEpochSecond(segmentTime).atZone(ZoneId.systemDefault()).toLocalDateTime();

    int hour = segmentDateTime.getHour();

    ShardConfig config = new ShardConfig();
    config.timeSeriesTableSize = 1;
    config.tagTableSize = 1;

    config.segmentStartTimes = new int[] {hour};
    config.shardIds = new int[] {0};
    config.shardCount = 1;

    storage =
        new EphemeralStorage(
            memoryInfoReader,
            config,
            metricRegistry,
            hashFunction,
            encoderFactory,
            dataStoreFactory,
            null,
            scheduledExecutorService);
    context = storage.context;
  }

  @Test
  void testSegmentTimeValidation() {

    assertFalse(storage.isDelayed(segmentTime, currentTimeHour));
    assertFalse(storage.isEarly(segmentTime, currentTimeHour));

    int earlySegmentTim = segmentTime + context.getSecondsInASegment();
    assertFalse(storage.isDelayed(earlySegmentTim, currentTimeHour));
    assertTrue(storage.isEarly(earlySegmentTim, currentTimeHour));

    int delayedSegmentTime = segmentTime - context.getSecondsInASegment();
    assertTrue(storage.isDelayed(delayedSegmentTime, currentTimeHour));
    assertFalse(storage.isEarly(delayedSegmentTime, currentTimeHour));
  }

  @ParameterizedTest(name = "[{index}] {3}")
  @MethodSource("getQueryTimeRangeForFirstSegment")
  void testGetSegmentTimes(
      final int startTime, final int endTime, final int expected, final String displayName) {
    long segmentTimes = context.getSegmentTimes(startTime, endTime);
    int actualSegmentTime = context.getFirstSegmentTime(segmentTimes);
    int actualSegmentCount = context.getSegmentCount(segmentTimes);
    int expectedCount = expected > 0 ? 1 : 0;
    assertEquals(expected, actualSegmentTime);
    assertEquals(expectedCount, actualSegmentCount);
  }

  @ParameterizedTest(name = "[{index}] {3}")
  @MethodSource("timestampsForTwoHourSegmentTime")
  void testTimeOfTwoHourSegment(
      final TimeseriesStorageContext storageContext,
      final int timeStampInSeconds,
      final int expected,
      final String displayName) {
    assertEquals(expected, storageContext.getSegmentTime(timeStampInSeconds));
  }

  @ParameterizedTest(name = "[{index}] {3}")
  @MethodSource("timestampsForOneHourSegmentTime")
  void testTimeOfOneHourSegment(
      final TimeseriesStorageContext storageContext,
      final int timeStampInSeconds,
      final int expected,
      final String displayName) {
    assertEquals(expected, storageContext.getSegmentTime(timeStampInSeconds));
  }

  private Stream<Arguments> timestampsForOneHourSegmentTime() {
    ShardConfig config = new ShardConfig();
    config.segmentSizeHour = 1;
    EphemeralStorage.EphemeralStorageContext storageContext =
        new EphemeralStorage.EphemeralStorageContext(config);

    int segmentTimeInSeconds = segmentTime;
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(config.segmentSizeHour);
    return Stream.of(
        arguments(
            storageContext, segmentTimeInSeconds, segmentTimeInSeconds, "timestamp == segmentTime"),
        arguments(
            storageContext,
            segmentTimeInSeconds - 10,
            segmentTimeInSeconds - secondsInASegment,
            "timestamp < segmentTime"),
        arguments(
            storageContext,
            segmentTimeInSeconds + 10,
            segmentTimeInSeconds,
            "segmentTime < timestamp < segmentEndTime"),
        arguments(
            storageContext,
            segmentTimeInSeconds + secondsInASegment - 1,
            segmentTimeInSeconds,
            "timestamp < segmentEndTime"),
        arguments(
            storageContext,
            segmentTimeInSeconds + secondsInASegment,
            segmentTimeInSeconds + secondsInASegment,
            "timestamp == segmentEndTime"));
  }

  private Stream<Arguments> timestampsForTwoHourSegmentTime() {
    ShardConfig config = new ShardConfig();
    config.segmentSizeHour = 2;
    EphemeralStorage.EphemeralStorageContext storageContext =
        new EphemeralStorage.EphemeralStorageContext(config);
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(2);
    int segmentTimeInSeconds = now - (now % secondsInASegment);
    return Stream.of(
        arguments(
            storageContext, segmentTimeInSeconds, segmentTimeInSeconds, "timestamp == segmentTime"),
        arguments(
            storageContext,
            segmentTimeInSeconds - 10,
            segmentTimeInSeconds - secondsInASegment,
            "timestamp < segmentTime"),
        arguments(
            storageContext,
            segmentTimeInSeconds + 10,
            segmentTimeInSeconds,
            "segmentTime < timestamp < segmentEndTime"),
        arguments(
            storageContext,
            segmentTimeInSeconds + secondsInASegment - 1,
            segmentTimeInSeconds,
            "timestamp < segmentEndTime"),
        arguments(
            storageContext,
            segmentTimeInSeconds + secondsInASegment,
            segmentTimeInSeconds + secondsInASegment,
            "timestamp == segmentEndTime"));
  }

  private Stream<Arguments> getQueryTimeRange() {
    int segmentStartTime = segmentTime;
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(2);
    int segmentEndTime = (segmentStartTime + secondsInASegment);
    return Stream.of(
        arguments(
            segmentStartTime,
            segmentEndTime,
            segmentStartTime,
            "queryStartTime == segmentStartTime and queryEndTime == segmentEndTime"),
        arguments(
            segmentStartTime,
            segmentEndTime + 10,
            segmentStartTime,
            "queryEndTime > segmentEndTime"),
        arguments(
            segmentStartTime - 10,
            segmentEndTime,
            segmentStartTime,
            "queryStartTime < segmentStartTime"),
        arguments(
            segmentStartTime + 10,
            segmentEndTime,
            segmentStartTime,
            "segmentStartTime > queryStartTime > segmentEndTime"),
        arguments(
            segmentStartTime - 10,
            segmentEndTime + 10,
            segmentStartTime,
            "segmentStartTime < queryStartTime > segmentEndTime"),
        arguments(
            segmentStartTime - 120,
            segmentStartTime - 60,
            0,
            "queryStartTime < segmentStartTime and queryEndTime < segmentStartTime"),
        arguments(
            segmentEndTime + 60,
            segmentEndTime + 120,
            0,
            "queryStartTime > segmentEndTime and queryEndTime > segmentEndTime"));
  }

  private Stream<Arguments> getQueryTimeRangeForFirstSegment() {
    int segmentStartTime = segmentTime;
    int secondsInASegment = (int) TimeUnit.HOURS.toSeconds(2);
    int segmentEndTime = (segmentStartTime + secondsInASegment);
    return Stream.of(
        arguments(
            segmentStartTime,
            segmentEndTime,
            segmentStartTime,
            "queryStartTime == segmentStartTime and queryEndTime == segmentEndTime"),
        arguments(
            segmentStartTime,
            segmentEndTime + 10,
            segmentStartTime,
            "queryEndTime > segmentEndTime"),
        arguments(
            segmentStartTime - 10,
            segmentEndTime,
            segmentStartTime,
            "queryStartTime < segmentStartTime"),
        arguments(
            segmentStartTime + 10,
            segmentEndTime,
            segmentStartTime,
            "segmentStartTime > queryStartTime > segmentEndTime"),
        arguments(
            segmentStartTime - 10,
            segmentEndTime + 10,
            segmentStartTime,
            "segmentStartTime < queryStartTime > segmentEndTime"),
        arguments(
            segmentStartTime - 120,
            segmentStartTime - 60,
            0,
            "queryStartTime < segmentStartTime and queryEndTime < segmentStartTime"),
        arguments(
            segmentEndTime + 60,
            segmentEndTime + 120,
            0,
            "queryStartTime > segmentEndTime and queryEndTime > segmentEndTime"));
  }
}
