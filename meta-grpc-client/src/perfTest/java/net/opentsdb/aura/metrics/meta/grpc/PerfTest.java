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

import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import myst.MystServiceGrpc;
import myst.QueryRequest;
import myst.TimeseriesResponse;
import net.opentsdb.aura.metrics.meta.MetaClient.StreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PerfTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PerfTest.class);


  public static void main(String[] args) throws InterruptedException, IOException {

    int threadCount = args.length >= 1 ? Integer.parseInt(args[0]) : 1;
    boolean blocking = args.length >= 2 ? Boolean.parseBoolean(args[1]) : true;
    boolean bigQuery = args.length >= 3 ? Boolean.parseBoolean(args[2]) : false;
    boolean shareClient = args.length >= 4 ? Boolean.parseBoolean(args[3]) : true;
    boolean localServer = args.length >= 5 ? Boolean.parseBoolean(args[4]) : true;
    boolean useDefaultClient = args.length >= 6 ? Boolean.parseBoolean(args[5]) : false;

    String host = localServer ? "localhost" : "10.214.168.140";
    int port = 9999;

    if (localServer) {
      MystServer server = new MystServer(port);
      server.start();
    }

    String queryBig = "{\"from\":0,\"to\":1,\"start\":1616508000,\"end\":1616509800,\"order\":\"ASCENDING\",\"type\":\"TIMESERIES\",\"limit\":100,\"group\":[\"flid\"],\"namespace\":\"Yamas\",\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":\"TagValueRegex\",\"key\":\"flid\",\"filter\":\"1111\"},{\"type\":\"TagValueRegex\",\"key\":\"node_function_type\",\"filter\":\"normal\"},{\"type\":\"MetricLiteral\",\"metric\":\"exch.bdr.Bids\"}]}}";
    String querySmall = "{\"from\":0,\"to\":1,\"start\":1616508000,\"end\":1616509800,\"order\":\"ASCENDING\",\"type\":\"TIMESERIES\",\"limit\":100,\"group\":[\"flid\"],\"namespace\":\"Yamas\",\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":\"Chain\",\"op\":\"OR\",\"filters\":[{\"type\":\"TagValueLiteralOr\",\"filter\":\"ats-proxy|ats\",\"key\":\"corp:Application\"}]},{\"type\":\"TagValueLiteralOr\",\"filter\":\"prod\",\"key\":\"corp:Environment\"},{\"type\":\"TagValueRegex\",\"filter\":\".*\",\"key\":\"InstanceId\"},{\"type\":\"MetricLiteral\",\"metric\":\"net.bytes_sent\"}]}}";

    AtomicReference<MetaGrpcClient> clientRef = new AtomicReference<>();
    if (shareClient) {
      clientRef.set(new MetaGrpcClient(host, port));
    }

    String query = bigQuery ? queryBig : querySmall;

    Thread[] threads = new Thread[threadCount];

    AtomicLong groupCount = new AtomicLong();
    AtomicLong parseErrorCount = new AtomicLong();

    AtomicLong totalRequest = new AtomicLong();
    AtomicLong totalFailure = new AtomicLong();

    LOGGER.info("Running threadCount: " + threadCount + " blocking: " + blocking + " bigQuery: " + bigQuery + " shareClient: " + shareClient + " localserver: " + localServer + " useDefaultClient: " + useDefaultClient);

    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    MystServiceGrpc.MystServiceBlockingStub blockingStub = MystServiceGrpc.newBlockingStub(channel);
    MystServiceGrpc.MystServiceStub asyncStub = MystServiceGrpc.newStub(channel);


    for (int i = 0; i < threadCount; i++) {

      Thread t = new Thread(() -> {

        if (useDefaultClient) {

          if (shareClient) {

            if (blocking) {
              while (true) {
                try {
                  QueryRequest queryRequest = QueryRequest.newBuilder().setQuery(query).build();
                  Iterator<TimeseriesResponse> itr = blockingStub.getTimeseries(queryRequest);
                  while (itr.hasNext()) {
                    TimeseriesResponse next = itr.next();
                    groupCount.getAndAdd(next.getGroupedTimeseriesCount());
                  }
                  totalRequest.getAndIncrement();
                } catch (Throwable throwable) {
                  throwable.printStackTrace();
                  totalFailure.getAndIncrement();
                }
              }
            } else {
              // add streaming call
              while (true) {
                try {
                  QueryRequest queryRequest = QueryRequest.newBuilder().setQuery(query).build();
                  asyncStub.getTimeseries(queryRequest, new StreamObserver<TimeseriesResponse>() {
                    @Override
                    public void onNext(TimeseriesResponse value) {
                      groupCount.getAndAdd(value.getGroupedTimeseriesCount());
                    }

                    @Override
                    public void onError(Throwable t) {
                      LOGGER.error("Parse error", t);
                      parseErrorCount.getAndIncrement();
                    }

                    @Override
                    public void onCompleted() {
                      totalRequest.getAndIncrement();
                    }
                  });

                } catch (Throwable throwable) {
                  LOGGER.error("Query error", throwable);
                  totalFailure.getAndIncrement();
                }
              }
            }

          } else {
            // client per thread
            if (blocking) {
              ManagedChannel ch = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
              MystServiceGrpc.MystServiceBlockingStub bs = MystServiceGrpc.newBlockingStub(ch);
              while (true) {
                try {
                  QueryRequest queryRequest = QueryRequest.newBuilder().setQuery(query).build();
                  Iterator<TimeseriesResponse> itr = bs.getTimeseries(queryRequest);
                  while (itr.hasNext()) {
                    TimeseriesResponse next = itr.next();
                    groupCount.getAndAdd(next.getGroupedTimeseriesCount());
                  }
                  totalRequest.getAndIncrement();
                } catch (Throwable throwable) {
                  throwable.printStackTrace();
                  totalFailure.getAndIncrement();
                }
              }
            } else {
              ManagedChannel ch = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
              MystServiceGrpc.MystServiceStub async = MystServiceGrpc.newStub(ch);
              while (true) {
                try {
                  QueryRequest queryRequest = QueryRequest.newBuilder().setQuery(query).build();
                  async.getTimeseries(queryRequest, new StreamObserver<TimeseriesResponse>() {
                    @Override
                    public void onNext(TimeseriesResponse value) {
                      groupCount.getAndAdd(value.getGroupedTimeseriesCount());
                    }

                    @Override
                    public void onError(Throwable t) {
                      LOGGER.error("Parse error", t);
                      parseErrorCount.getAndIncrement();
                    }

                    @Override
                    public void onCompleted() {
                      totalRequest.getAndIncrement();
                    }
                  });

                } catch (Throwable throwable) {
                  LOGGER.error("Query error", throwable);
                  totalFailure.getAndIncrement();
                }
              }
            }
          }


        } else {
          // Use the optimized client
          MetaGrpcClient client;
          if (!shareClient) {
            client = new MetaGrpcClient(host, port);
          } else {
            client = clientRef.get();
          }
          if (blocking) {
            while (true) {
              try {
                Iterator<DefaultMetaTimeSeriesQueryResult> itr = client.getTimeseries(query);
                while (itr.hasNext()) {
                  DefaultMetaTimeSeriesQueryResult next = itr.next();
                  groupCount.getAndAdd(next.numGroups());
                }
                totalRequest.getAndIncrement();
              } catch (Throwable throwable) {
                throwable.printStackTrace();
                totalFailure.getAndIncrement();
              }
            }
          } else {
            StreamConsumer consumer = new StreamConsumer<DefaultMetaTimeSeriesQueryResult>() {
              @Override
              public void onNext(DefaultMetaTimeSeriesQueryResult result) {
                groupCount.getAndAdd(result.numGroups());
              }

              @Override
              public void onError(Throwable t) {
                t.printStackTrace();
                parseErrorCount.getAndIncrement();
              }

              @Override
              public void onCompleted() {
                totalRequest.getAndIncrement();
              }
            };
            while (true) {
              try {
                client.getTimeseries(query, consumer);
              } catch (Throwable throwable) {
                throwable.printStackTrace();
                totalFailure.getAndIncrement();
              }
            }
          }
        }


      });

      threads[i] = t;
    }

    for (int i = 0; i < threadCount; i++) {
      Thread t = threads[i];
      t.setName("Query-thread-" + (i + 1));
      t.setDaemon(true);
      t.start();
    }

    Thread rt = new Thread(() -> {
      long prevTotalReq = 0;
      long prevTotalFailure = 0;
      long prevParseErrorCount = 0;

      while (true) {
        try {
          Thread.sleep(10_000);

          long currTotalRequest = totalRequest.get();
          double qps = (currTotalRequest - prevTotalReq) / 10.0;
          prevTotalReq = currTotalRequest;

          long currTotalFailure = totalFailure.get();
          double failureRate = (currTotalFailure - prevTotalFailure) / 10.0;
          prevTotalFailure = currTotalFailure;

          long currParseErrorCount = parseErrorCount.get();
          double parseErrorRate = (currParseErrorCount - prevParseErrorCount) / 10.0;
          prevParseErrorCount = currParseErrorCount;

          StringBuilder sb = new StringBuilder();
          sb.append("TotalRequest: ").append(currTotalRequest).append(" TotalFailure: ").append(currTotalFailure).append(" TotalParseError: ").append(currParseErrorCount)
              .append(" QPS: ").append(qps).append(" failureRate: ").append(failureRate).append(" /sec parseErrorRate: ").append(parseErrorRate).append(" /sec").append(" TotalGroupCount: ").append(groupCount.get());
          LOGGER.info(sb.toString());

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    rt.setName("Stats-reporter");
    rt.setDaemon(true);
    rt.start();

    for (Thread t : threads) {
      t.join();
    }

  }

}
