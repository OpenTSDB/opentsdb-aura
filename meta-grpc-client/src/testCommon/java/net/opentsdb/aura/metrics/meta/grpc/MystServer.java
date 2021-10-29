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
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;
import myst.Dictionary;
import myst.GroupedTimeseries;
import myst.MystServiceGrpc;
import myst.QueryRequest;
import myst.Timeseries;
import myst.TimeseriesResponse;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MystServer {

  private static final Logger logger = LoggerFactory.getLogger(MystServer.class);

  private Server server;
  private final int port;
  private final MystServiceGrpc.MystServiceImplBase mystService;
  private boolean compress;
  private static Random random = new Random();
  private static TimeseriesResponse[] responseStream;
  private static int streamCount;
  private static final int[] timestampSequence = new int[] {0, 1};

  public MystServer(final int port) {
    this(port, true);
  }

  public MystServer(final int port, final boolean compress) {
    this(port, new MystServiceImpl(), compress);
  }

  public MystServer(final int port, MystServiceGrpc.MystServiceImplBase mystService) {
    this(port, mystService, true);
  }

  public MystServer(
      final int port, MystServiceGrpc.MystServiceImplBase mystService, boolean compress) {
    this.port = port;
    this.mystService = mystService;
    this.compress = compress;
  }

  public void start() throws IOException {
    /* The port on which the server should run */
    ServerBuilder<?> builder = ServerBuilder.forPort(port).addService(mystService);
    if (compress) {
      builder.intercept(
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
              call.setCompression("gzip");
              return next.startCall(call, headers);
            }
          });
    }
    server = builder.build().start();
    logger.info("Server started, listening on {} compression enabled: {}", port, compress);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
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

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {

    boolean compress = true;
    int hashPerStream = 100_000;
    int totalHash = 3000_000;
    int groupCount = 1;

    if (args != null) {
      int length = args.length;
      if (length >= 1) {
        compress = Boolean.parseBoolean(args[0]);
      }
      if (length >= 2) {
        hashPerStream = Integer.parseInt(args[1]);
      }
      if (length >= 3) {
        totalHash = Integer.parseInt(args[2]);
      }
    }

    streamCount = totalHash / hashPerStream;
    responseStream = new TimeseriesResponse[streamCount];

    ByteString bitMap = ByteString.copyFrom(getBitMap(new int[] {1634558400, 1634580000}));
    for (int stream = 0; stream < streamCount; stream++) {
      TimeseriesResponse.Builder tsResponseBuilder = TimeseriesResponse.newBuilder();
      Dictionary dict = Dictionary.newBuilder().build();
      for (int i = 0; i < groupCount; i++) {
        GroupedTimeseries.Builder groupBuilder = GroupedTimeseries.newBuilder();
        for (int j = 0; j < hashPerStream; j++) {
          groupBuilder.addTimeseries(
              Timeseries.newBuilder().setHash(random.nextLong()).setEpochBitmap(bitMap));
        }
        GroupedTimeseries group = groupBuilder.build();
        tsResponseBuilder.addGroupedTimeseries(group);
      }
      tsResponseBuilder.setDict(dict);
      TimeseriesResponse response = tsResponseBuilder.build();
      responseStream[stream] = response;
    }

    int port = 9999;
    final MystServer server = new MystServer(port, compress);
    server.start();
    server.blockUntilShutdown();
  }

  static class MystServiceImpl extends MystServiceGrpc.MystServiceImplBase {

    @Override
    public void getTimeseries(
        QueryRequest request, StreamObserver<TimeseriesResponse> responseObserver) {
      String query = request.getQuery();
      logger.debug("Query: " + query);

      for (int stream = 0; stream < streamCount; stream++) {
        responseObserver.onNext(responseStream[stream]);
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
