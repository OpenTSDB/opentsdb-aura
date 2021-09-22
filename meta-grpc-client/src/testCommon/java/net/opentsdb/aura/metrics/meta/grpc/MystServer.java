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

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import myst.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.roaringbitmap.RoaringBitmap;


public class MystServer {

  private static final Logger logger = LoggerFactory.getLogger(MystServer.class);

  private Server server;
  private final int port;
  private final MystServiceGrpc.MystServiceImplBase mystService;

  private static final int[] timestampSequence = new int[]{0,1};

  public MystServer(final int port) {
    this(port, new MystServiceImpl());
  }

  public MystServer(final int port, MystServiceGrpc.MystServiceImplBase mystService) {
    this.port = port;
    this.mystService = mystService;
  }

  public void start() throws IOException {
    /* The port on which the server should run */
    server = ServerBuilder.forPort(port)
        .addService(mystService)
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Use stderr here since the logger may have been reset by its JVM shutdown hook.
      System.err.println("*** shutting down gRPC server since JVM is shutting down");
      try {
        MystServer.this.stop();
      } catch (InterruptedException e) {
        e.printStackTrace(System.err);
      }
      System.err.println("*** server shut down");
    }));
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    int port = 50051;
    final MystServer server = new MystServer(port);
    server.start();
    server.blockUntilShutdown();
  }

  static class MystServiceImpl extends MystServiceGrpc.MystServiceImplBase {

    @Override
    public void getTimeseries(QueryRequest request, StreamObserver<TimeseriesResponse> responseObserver) {
      String query = request.getQuery();
      logger.debug("Query: " + query);

      int shardCount = 30;
      int groupCount = 1;
      int tsInAGroup = 10;

      for (int shard = 0; shard < shardCount; shard++) {
        TimeseriesResponse.Builder tsResponseBuilder = TimeseriesResponse.newBuilder();
        Dictionary.Builder dictBuilder = Dictionary.newBuilder();
        for (int i = 0; i < groupCount; i++) {
          GroupedTimeseries.Builder groupBuilder = GroupedTimeseries.newBuilder();
          for (int j = 0; j < tsInAGroup; j++) {
            long groupKey = i + j;
            groupBuilder.addGroup(groupKey);
            groupBuilder.addTimeseries(
                    Timeseries.newBuilder()
                            .setHash(j + 1)
                            .setEpochBitmap(ByteString.copyFrom(getBitMap(timestampSequence))));
          }
          GroupedTimeseries group = groupBuilder.build();
          tsResponseBuilder.addGroupedTimeseries(group);
        }
        Dictionary dict = dictBuilder.build();
        dictBuilder.putDict(0, "0");
        tsResponseBuilder.setDict(dict);
        responseObserver.onNext(tsResponseBuilder.build());
      }
      responseObserver.onCompleted();
    }
  }

  public static byte[] getBitMap(int[] ts) {
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(ts);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      roaringBitmap.serialize(dataOutputStream);
      dataOutputStream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }


    return byteArrayOutputStream.toByteArray();
  }

}
