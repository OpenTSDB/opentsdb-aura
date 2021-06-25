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

package net.opentsdb.aura.metrics.meta.grpc;

import io.grpc.stub.StreamObserver;
import myst.Dictionary;
import myst.GroupedTimeseries;
import myst.MystServiceGrpc;
import myst.QueryRequest;
import myst.TimeseriesResponse;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.MetaClient;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult.GroupResult;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult.GroupResult.TagHashes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class MetaGrpcClientTest {

  final String host = "localhost";
  final int port = 8080;
  private MystServer server;

  @AfterEach
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testGetTimeseriesBlockingFromSingleShard() throws IOException {

    GroupedTimeseries group1 =
        GroupedTimeseries.newBuilder()
            .addGroup(1)
            .addGroup(2)
            .addTimeseries(2)
            .addTimeseries(4)
            .build();
    GroupedTimeseries group2 =
        GroupedTimeseries.newBuilder()
            .addGroup(3)
            .addGroup(4)
            .addTimeseries(6)
            .addTimeseries(8)
            .build();

    Dictionary dictionary =
        Dictionary.newBuilder()
            .putDict(1, String.valueOf(1))
            .putDict(2, String.valueOf(2))
            .putDict(3, String.valueOf(3))
            .putDict(4, String.valueOf(4))
            .build();
    TimeseriesResponse response =
        TimeseriesResponse.newBuilder()
            .addGroupedTimeseries(group1)
            .addGroupedTimeseries(group2)
            .setDict(dictionary)
            .build();

    MystServiceGrpc.MystServiceImplBase mystService =
        new MystServiceGrpc.MystServiceImplBase() {
          @Override
          public void getTimeseries(
              QueryRequest request, StreamObserver<TimeseriesResponse> responseObserver) {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }
        };

    server = new MystServer(port, mystService);
    server.start();

    MetaGrpcClient client = new MetaGrpcClient(host, port);
    Iterator<DefaultMetaTimeSeriesQueryResult> responseItr = client.getTimeseries("test-query");
    int count = 0;
    while (responseItr.hasNext()) {
      count++;
      assertEquals(1, count); // assert Iterator has only one entry

      DefaultMetaTimeSeriesQueryResult clientResponse = responseItr.next();
      assertEquals(2, clientResponse.numGroups());

      GroupResult groupResult1 = clientResponse.getGroup(0);

      assertEquals(2, groupResult1.numHashes());
      TagHashes tagHashes1 = groupResult1.tagHashes();
      assertEquals(2, tagHashes1.size());
      assertEquals(1, tagHashes1.next());
      assertEquals(2, tagHashes1.next());
      assertEquals(2, groupResult1.getHash(0));
      assertEquals(4, groupResult1.getHash(1));

      GroupResult groupResult2 = clientResponse.getGroup(1);
      assertEquals(2, groupResult2.numHashes());
      TagHashes tagHashes2 = groupResult2.tagHashes();
      assertEquals(2, tagHashes2.size());
      assertEquals(3, tagHashes2.next());
      assertEquals(4, tagHashes2.next());
      assertEquals(6, groupResult2.getHash(0));
      assertEquals(8, groupResult2.getHash(1));

      assertEquals("1", clientResponse.getStringForHash(1));
      assertEquals("2", clientResponse.getStringForHash(2));
      assertEquals("3", clientResponse.getStringForHash(3));
      assertEquals("4", clientResponse.getStringForHash(4));
    }
  }

  @Test
  public void testGetTimeseriesAsyncFromSingleShard() throws IOException, InterruptedException {

    GroupedTimeseries group1 =
        GroupedTimeseries.newBuilder()
            .addGroup(1)
            .addGroup(2)
            .addTimeseries(2)
            .addTimeseries(4)
            .build();
    GroupedTimeseries group2 =
        GroupedTimeseries.newBuilder()
            .addGroup(3)
            .addGroup(4)
            .addTimeseries(6)
            .addTimeseries(8)
            .build();

    Dictionary dictionary =
        Dictionary.newBuilder()
            .putDict(1, String.valueOf(1))
            .putDict(2, String.valueOf(2))
            .putDict(3, String.valueOf(3))
            .putDict(4, String.valueOf(4))
            .build();
    TimeseriesResponse response =
        TimeseriesResponse.newBuilder()
            .addGroupedTimeseries(group1)
            .addGroupedTimeseries(group2)
            .setDict(dictionary)
            .setStreams(1)
            .build();

    MystServiceGrpc.MystServiceImplBase mystService =
        new MystServiceGrpc.MystServiceImplBase() {
          @Override
          public void getTimeseries(
              QueryRequest request, StreamObserver<TimeseriesResponse> responseObserver) {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }
        };

    server = new MystServer(port, mystService);
    server.start();

    MetaGrpcClient client = new MetaGrpcClient(host, port);

    AtomicInteger counter = new AtomicInteger();
    AtomicBoolean isComplete = new AtomicBoolean();

    CountDownLatch countDown =
        client.getTimeseries(
            "test-query",
            new MetaClient.StreamConsumer<DefaultMetaTimeSeriesQueryResult>() {

              @Override
              public void onNext(DefaultMetaTimeSeriesQueryResult clientResponse) {
                counter.getAndIncrement();

                assertEquals(2, clientResponse.numGroups());

                MetaTimeSeriesQueryResult.GroupResult groupResult1 = clientResponse.getGroup(0);

                assertEquals(2, groupResult1.numHashes());
                MetaTimeSeriesQueryResult.GroupResult.TagHashes tagHashes1 =
                    groupResult1.tagHashes();
                assertEquals(2, tagHashes1.size());
                assertEquals(1, tagHashes1.next());
                assertEquals(2, tagHashes1.next());
                assertEquals(2, groupResult1.getHash(0));
                assertEquals(4, groupResult1.getHash(1));

                MetaTimeSeriesQueryResult.GroupResult groupResult2 = clientResponse.getGroup(1);
                assertEquals(2, groupResult2.numHashes());
                MetaTimeSeriesQueryResult.GroupResult.TagHashes tagHashes2 =
                    groupResult2.tagHashes();
                assertEquals(2, tagHashes2.size());
                assertEquals(3, tagHashes2.next());
                assertEquals(4, tagHashes2.next());
                assertEquals(6, groupResult2.getHash(0));
                assertEquals(8, groupResult2.getHash(1));

                assertEquals("1", clientResponse.getStringForHash(1));
                assertEquals("2", clientResponse.getStringForHash(2));
                assertEquals("3", clientResponse.getStringForHash(3));
                assertEquals("4", clientResponse.getStringForHash(4));
              }

              @Override
              public void onError(Throwable t) {
                fail(t);
              }

              @Override
              public void onCompleted() {
                isComplete.set(true);
              }
            });

    countDown.await();
    assertEquals(1, counter.get()); // assert only one stream
    assertTrue(isComplete.get());
  }

  @Test
  public void testGetTimeseriesBlockingFromMultipleShards() throws IOException {

    int shardCount = 5;
    MystServiceGrpc.MystServiceImplBase mystService =
        new MystServiceGrpc.MystServiceImplBase() {
          @Override
          public void getTimeseries(
              QueryRequest request, StreamObserver<TimeseriesResponse> responseObserver) {
            for (int i = 0; i < shardCount; i++) {
              long one = (long) (1 * (Math.pow(10, i)));
              long two = (long) (2 * (Math.pow(10, i)));
              long three = (long) (3 * (Math.pow(10, i)));
              long four = (long) (4 * (Math.pow(10, i)));
              long six = (long) (6 * (Math.pow(10, i)));
              long eight = (long) (8 * (Math.pow(10, i)));

              GroupedTimeseries group1 =
                  GroupedTimeseries.newBuilder()
                      .addGroup(one)
                      .addGroup(two)
                      .addTimeseries(two)
                      .addTimeseries(four)
                      .build();
              GroupedTimeseries group2 =
                  GroupedTimeseries.newBuilder()
                      .addGroup(three)
                      .addGroup(four)
                      .addTimeseries(six)
                      .addTimeseries(eight)
                      .build();

              Dictionary dictionary =
                  Dictionary.newBuilder()
                      .putDict(one, String.valueOf(one))
                      .putDict(two, String.valueOf(two))
                      .putDict(three, String.valueOf(three))
                      .putDict(four, String.valueOf(four))
                      .build();
              TimeseriesResponse response =
                  TimeseriesResponse.newBuilder()
                      .addGroupedTimeseries(group1)
                      .addGroupedTimeseries(group2)
                      .setDict(dictionary)
                      .build();
              responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
          }
        };

    server = new MystServer(port, mystService);
    server.start();

    MetaGrpcClient client = new MetaGrpcClient(host, port);
    Iterator<DefaultMetaTimeSeriesQueryResult> responseItr = client.getTimeseries("test-query");
    int count = 0;
    while (responseItr.hasNext()) {
      long one = (long) (1 * (Math.pow(10, count)));
      long two = (long) (2 * (Math.pow(10, count)));
      long three = (long) (3 * (Math.pow(10, count)));
      long four = (long) (4 * (Math.pow(10, count)));
      long six = (long) (6 * (Math.pow(10, count)));
      long eight = (long) (8 * (Math.pow(10, count)));

      count++;
      assertTrue(count <= shardCount);

      DefaultMetaTimeSeriesQueryResult result = responseItr.next();
      assertEquals(2, result.numGroups());

      GroupResult groupResult1 = result.getGroup(0);
      assertEquals(2, groupResult1.numHashes());
      TagHashes tagHashes1 = groupResult1.tagHashes();
      assertEquals(one, tagHashes1.next());
      assertEquals(two, tagHashes1.next());
      assertEquals(two, groupResult1.getHash(0));
      assertEquals(four, groupResult1.getHash(1));

      GroupResult groupResult2 = result.getGroup(1);

      assertEquals(2, groupResult2.numHashes());
      TagHashes tagHashes2 = groupResult2.tagHashes();
      assertEquals(three, tagHashes2.next());
      assertEquals(four, tagHashes2.next());
      assertEquals(six, groupResult2.getHash(0));
      assertEquals(eight, groupResult2.getHash(1));

      assertEquals(String.valueOf(one), result.getStringForHash(one));
      assertEquals(String.valueOf(two), result.getStringForHash(two));
      assertEquals(String.valueOf(three), result.getStringForHash(three));
      assertEquals(String.valueOf(four), result.getStringForHash(four));
    }
  }

  @Test
  public void testGetTimeseriesAsyncFromMultipleShards() throws IOException, InterruptedException {

    int shardCount = 5;
    MystServiceGrpc.MystServiceImplBase mystService =
        new MystServiceGrpc.MystServiceImplBase() {
          @Override
          public void getTimeseries(
              QueryRequest request, StreamObserver<TimeseriesResponse> responseObserver) {
            for (int i = 0; i < shardCount; i++) {
              long one = (long) (1 * (Math.pow(10, i)));
              long two = (long) (2 * (Math.pow(10, i)));
              long three = (long) (3 * (Math.pow(10, i)));
              long four = (long) (4 * (Math.pow(10, i)));
              long six = (long) (6 * (Math.pow(10, i)));
              long eight = (long) (8 * (Math.pow(10, i)));

              GroupedTimeseries group1 =
                  GroupedTimeseries.newBuilder()
                      .addGroup(one)
                      .addGroup(two)
                      .addTimeseries(two)
                      .addTimeseries(four)
                      .build();
              GroupedTimeseries group2 =
                  GroupedTimeseries.newBuilder()
                      .addGroup(three)
                      .addGroup(four)
                      .addTimeseries(six)
                      .addTimeseries(eight)
                      .build();

              Dictionary dictionary =
                  Dictionary.newBuilder()
                      .putDict(one, String.valueOf(one))
                      .putDict(two, String.valueOf(two))
                      .putDict(three, String.valueOf(three))
                      .putDict(four, String.valueOf(four))
                      .build();
              TimeseriesResponse response =
                  TimeseriesResponse.newBuilder()
                      .addGroupedTimeseries(group1)
                      .addGroupedTimeseries(group2)
                      .setDict(dictionary)
                      .setStreams(shardCount)
                      .build();
              responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
          }
        };

    server = new MystServer(port, mystService);
    server.start();

    MetaGrpcClient client = new MetaGrpcClient(host, port);

    AtomicInteger counter = new AtomicInteger();
    AtomicBoolean isComplete = new AtomicBoolean();

    CountDownLatch countDown =
        client.getTimeseries(
            "test-query",
            new MetaClient.StreamConsumer<DefaultMetaTimeSeriesQueryResult>() {

              @Override
              public void onNext(DefaultMetaTimeSeriesQueryResult result) {
                long one = (long) (1 * (Math.pow(10, counter.get())));
                long two = (long) (2 * (Math.pow(10, counter.get())));
                long three = (long) (3 * (Math.pow(10, counter.get())));
                long four = (long) (4 * (Math.pow(10, counter.get())));
                long six = (long) (6 * (Math.pow(10, counter.get())));
                long eight = (long) (8 * (Math.pow(10, counter.get())));

                counter.getAndIncrement();
                assertTrue(counter.get() <= shardCount);

                assertEquals(2, result.numGroups());

                MetaTimeSeriesQueryResult.GroupResult groupResult1 = result.getGroup(0);
                assertEquals(2, groupResult1.numHashes());
                MetaTimeSeriesQueryResult.GroupResult.TagHashes tagHashes1 =
                    groupResult1.tagHashes();
                assertEquals(one, tagHashes1.next());
                assertEquals(two, tagHashes1.next());
                assertEquals(two, groupResult1.getHash(0));
                assertEquals(four, groupResult1.getHash(1));

                MetaTimeSeriesQueryResult.GroupResult groupResult2 = result.getGroup(1);

                assertEquals(2, groupResult2.numHashes());
                MetaTimeSeriesQueryResult.GroupResult.TagHashes tagHashes2 =
                    groupResult2.tagHashes();
                assertEquals(three, tagHashes2.next());
                assertEquals(four, tagHashes2.next());
                assertEquals(six, groupResult2.getHash(0));
                assertEquals(eight, groupResult2.getHash(1));

                assertEquals(String.valueOf(one), result.getStringForHash(one));
                assertEquals(String.valueOf(two), result.getStringForHash(two));
                assertEquals(String.valueOf(three), result.getStringForHash(three));
                assertEquals(String.valueOf(four), result.getStringForHash(four));
              }

              @Override
              public void onError(Throwable t) {
                fail(t);
              }

              @Override
              public void onCompleted() {
                isComplete.set(true);
              }
            });

    assertTrue(countDown.await(10, TimeUnit.SECONDS));
    assertEquals(5, counter.get());
    assertTrue(isComplete.get());
  }

  @Test
  public void testGetTimeseriesAsyncDuringAShardError() throws IOException, InterruptedException {

    int shardCount = 5;
    MystServiceGrpc.MystServiceImplBase mystService =
        new MystServiceGrpc.MystServiceImplBase() {
          @Override
          public void getTimeseries(
              QueryRequest request, StreamObserver<TimeseriesResponse> responseObserver) {
            for (int i = 0; i < shardCount; i++) {
              long one = (long) (1 * (Math.pow(10, i)));
              long two = (long) (2 * (Math.pow(10, i)));
              long three = (long) (3 * (Math.pow(10, i)));
              long four = (long) (4 * (Math.pow(10, i)));
              long six = (long) (6 * (Math.pow(10, i)));
              long eight = (long) (8 * (Math.pow(10, i)));

              GroupedTimeseries group1 =
                  GroupedTimeseries.newBuilder()
                      .addGroup(one)
                      .addGroup(two)
                      .addTimeseries(two)
                      .addTimeseries(four)
                      .build();
              GroupedTimeseries group2 =
                  GroupedTimeseries.newBuilder()
                      .addGroup(three)
                      .addGroup(four)
                      .addTimeseries(six)
                      .addTimeseries(eight)
                      .build();

              Dictionary dictionary =
                  Dictionary.newBuilder()
                      .putDict(one, String.valueOf(one))
                      .putDict(two, String.valueOf(two))
                      .putDict(three, String.valueOf(three))
                      .putDict(four, String.valueOf(four))
                      .build();
              TimeseriesResponse response =
                  TimeseriesResponse.newBuilder()
                      .addGroupedTimeseries(group1)
                      .addGroupedTimeseries(group2)
                      .setDict(dictionary)
                      .setStreams(shardCount)
                      .build();

              if (i == shardCount - 1) {
                responseObserver.onError(new Exception("Error reading Shard: " + i));
              } else {
                responseObserver.onNext(response);
              }
            }
          }
        };

    server = new MystServer(port, mystService);
    server.start();

    MetaGrpcClient client = new MetaGrpcClient(host, port);

    AtomicInteger counter = new AtomicInteger();
    AtomicBoolean isError = new AtomicBoolean();

    CountDownLatch countDown =
        client.getTimeseries(
            "test-query",
            new MetaClient.StreamConsumer<DefaultMetaTimeSeriesQueryResult>() {

              @Override
              public void onNext(DefaultMetaTimeSeriesQueryResult result) {
                long one = (long) (1 * (Math.pow(10, counter.get())));
                long two = (long) (2 * (Math.pow(10, counter.get())));
                long three = (long) (3 * (Math.pow(10, counter.get())));
                long four = (long) (4 * (Math.pow(10, counter.get())));
                long six = (long) (6 * (Math.pow(10, counter.get())));
                long eight = (long) (8 * (Math.pow(10, counter.get())));

                counter.getAndIncrement();
                assertTrue(counter.get() <= shardCount);

                assertEquals(2, result.numGroups());

                MetaTimeSeriesQueryResult.GroupResult groupResult1 = result.getGroup(0);
                assertEquals(2, groupResult1.numHashes());
                MetaTimeSeriesQueryResult.GroupResult.TagHashes tagHashes1 =
                    groupResult1.tagHashes();
                assertEquals(one, tagHashes1.next());
                assertEquals(two, tagHashes1.next());
                assertEquals(two, groupResult1.getHash(0));
                assertEquals(four, groupResult1.getHash(1));

                MetaTimeSeriesQueryResult.GroupResult groupResult2 = result.getGroup(1);

                assertEquals(2, groupResult2.numHashes());
                MetaTimeSeriesQueryResult.GroupResult.TagHashes tagHashes2 =
                    groupResult2.tagHashes();
                assertEquals(three, tagHashes2.next());
                assertEquals(four, tagHashes2.next());
                assertEquals(six, groupResult2.getHash(0));
                assertEquals(eight, groupResult2.getHash(1));

                assertEquals(String.valueOf(one), result.getStringForHash(one));
                assertEquals(String.valueOf(two), result.getStringForHash(two));
                assertEquals(String.valueOf(three), result.getStringForHash(three));
                assertEquals(String.valueOf(four), result.getStringForHash(four));
              }

              @Override
              public void onError(Throwable t) {
                isError.set(true);
              }

              @Override
              public void onCompleted() {
                fail("Shard failure expected");
              }
            });

    assertFalse(countDown.await(10, TimeUnit.SECONDS));
    assertEquals(4, counter.get());
    assertTrue(isError.get());
  }
}
