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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import myst.Dictionary;
import myst.GroupedTimeseries;
import myst.MystServiceGrpc;
import myst.QueryRequest;
import myst.TimeseriesResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Iterator;

@RunWith(JUnit4.class)
public class GrpcServerTest {

  private String host = "localhost";
  private int port = 50051;

  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Test
  public void testGetTimeseries() {

    myst.GroupedTimeseries group1 = myst.GroupedTimeseries.newBuilder().addGroup(1).addGroup(2).addTimeseries(2).addTimeseries(4).build();
    myst.GroupedTimeseries group2 = myst.GroupedTimeseries.newBuilder().addGroup(3).addGroup(4).addTimeseries(6).addTimeseries(8).build();

    Dictionary dictionary = Dictionary.newBuilder().putDict(1, String.valueOf(1)).putDict(2, String.valueOf(2)).putDict(3, String.valueOf(3)).putDict(4, String.valueOf(4)).build();
    TimeseriesResponse response = TimeseriesResponse.newBuilder().addGroupedTimeseries(group1).addGroupedTimeseries(group2).setDict(dictionary).build();

    MystServiceGrpc.MystServiceImplBase mystService = new MystServiceGrpc.MystServiceImplBase() {
      @Override
      public void getTimeseries(QueryRequest request, StreamObserver<TimeseriesResponse> responseObserver) {
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    };

    Server server = ServerBuilder
        .forPort(port)
        .addService(mystService)
        .build();

    new Thread(() -> {
      try {
        server.start();
        server.awaitTermination();
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    }).start();


    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    MystServiceGrpc.MystServiceBlockingStub blockingStub = MystServiceGrpc.newBlockingStub(channel);

    QueryRequest query = QueryRequest.newBuilder().setQuery("test-query").build();
    Iterator<TimeseriesResponse> responseItr = blockingStub.getTimeseries(query);
    int shard = 0;
    while (responseItr.hasNext()) {
      TimeseriesResponse actualResponse = responseItr.next();
      int groupedTSCount = actualResponse.getGroupedTimeseriesCount();
      System.out.println("**************");
      System.out.println("Shard: " + shard + "\n");
      for (int i = 0; i < groupedTSCount; i++) {
        GroupedTimeseries groupedTimeseries = actualResponse.getGroupedTimeseries(i);
        System.out.println(groupedTimeseries);
      }
      Dictionary dict = actualResponse.getDict();
      System.out.println(dict);
      shard++;
    }
  }

  @Test
  public void testInprocess() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(InProcessServerBuilder
        .forName(serverName).directExecutor().addService(new MystServer.MystServiceImpl()).build().start());

    MystServiceGrpc.MystServiceBlockingStub blockingStub = MystServiceGrpc.newBlockingStub(grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    QueryRequest query = QueryRequest.newBuilder().setQuery("test-query").build();
    Iterator<TimeseriesResponse> responseItr = blockingStub.getTimeseries(query);
    int shard = 0;
    while (responseItr.hasNext()) {
      TimeseriesResponse response = responseItr.next();
      int groupedTSCount = response.getGroupedTimeseriesCount();
      System.out.println("**************");
      System.out.println("Shard: " + shard + "\n");
      for (int i = 0; i < groupedTSCount; i++) {
        GroupedTimeseries groupedTimeseries = response.getGroupedTimeseries(i);
        System.out.println(groupedTimeseries);
      }
      Dictionary dict = response.getDict();
      System.out.println(dict);
      shard++;
    }
  }
}
