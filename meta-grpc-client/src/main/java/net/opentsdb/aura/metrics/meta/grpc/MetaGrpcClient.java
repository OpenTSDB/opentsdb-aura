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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.StreamObserver;
import myst.MystServiceGrpc;
import myst.QueryRequest;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.MetaClient;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;

public class MetaGrpcClient implements MetaClient<DefaultMetaTimeSeriesQueryResult> {

  private static final int DEFAULT_CON_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(5);

  private final ManagedChannel channel;
  private final MetaResultBlockingStub blockingStub;
  private final MetaResultAsyncSub asyncSub;

  public MetaGrpcClient(final String host, final int port) {
    this(host, port, DEFAULT_CON_TIMEOUT);
  }

  public MetaGrpcClient(final String host, final int port, final int contTimeout) {
    this.channel =
        NettyChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .enableRetry()
            .maxRetryAttempts(3)
            .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, contTimeout)
            .build();
    this.blockingStub = new MetaResultBlockingStub(channel);
    this.asyncSub = new MetaResultAsyncSub(channel);
  }

  @Override
  public Iterator<DefaultMetaTimeSeriesQueryResult> getTimeseries(String query) {
    QueryRequest queryRequest = QueryRequest.newBuilder().setQuery(query).build();
    return blockingStub.getTimeseries(queryRequest);
  }

  @Override
  public CountDownLatch getTimeseries(
      String query, StreamConsumer<DefaultMetaTimeSeriesQueryResult> consumer) {
    QueryRequest queryRequest = QueryRequest.newBuilder().setQuery(query).build();
    final MystCountDownLatch countDownLatch = new MystCountDownLatch();
    asyncSub.getTimeseries(
        queryRequest,
        new StreamObserver<DefaultMetaTimeSeriesQueryResult>() {

          @Override
          public void onNext(DefaultMetaTimeSeriesQueryResult result) {
            consumer.onNext(result);
            int numStreams = result.totalResults();
            if (!countDownLatch.setCount(numStreams)) {
              countDownLatch.countDown();
            }
          }

          @Override
          public void onError(Throwable t) {
            consumer.onError(t);
            countDownLatch.fail();
          }

          @Override
          public void onCompleted() {
            consumer.onCompleted();
            countDownLatch.countDown();
          }
        });
    return countDownLatch;
  }

  /**
   * An {@link AtomicInteger} based implementation of {@link CountDownLatch}, specially designed to
   * work for Myst server. The count of the latch should match with the number of response streams.
   * The number of stream is dynamic and the value is passed along with each stream. The counter
   * will be initialized with -1. Upon arrival of the first stream, the actual count is set. For the
   * subsequent streams and on completion it will count down.
   */
  private static class MystCountDownLatch extends CountDownLatch {

    /**
     * The number of nanoseconds for which it is faster to spin rather than to use timed park. A
     * rough estimate suffices to improve responsiveness with very short timeouts.
     */
    private static final long spinForTimeoutThreshold = 1000L;

    private final AtomicInteger counter;

    private volatile Thread blocked;
    private volatile boolean failed;

    private MystCountDownLatch() {
      super(0);
      this.counter = new AtomicInteger(-1);
    }

    private boolean setCount(int count) {
      return counter.compareAndSet(-1, count);
    }

    public void fail() {
      this.failed = true;
      this.counter.set(0);
      LockSupport.unpark(blocked);
    }

    @Override
    public void countDown() {
      counter.getAndDecrement();
      LockSupport.unpark(blocked);
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
      long nanosTimeout = unit.toNanos(timeout);
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      if (nanosTimeout <= 0L) {
        return false;
      }
      final long deadline = System.nanoTime() + nanosTimeout;

      for (; ; ) {
        if (counter.get() == 0) {
          return !failed;
        }
        nanosTimeout = deadline - System.nanoTime();
        if (nanosTimeout <= 0L) {
          return false;
        }
        if (nanosTimeout > spinForTimeoutThreshold) {
          this.blocked = Thread.currentThread();
          LockSupport.parkNanos(this, nanosTimeout);
        }

        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
      }
    }

    @Override
    public void await() throws InterruptedException {

      for (; ; ) {
        if (counter.get() == 0) {
          return;
        }
        if (parkAndCheckInterrupt()) {
          throw new InterruptedException();
        }
      }
    }

    private final boolean parkAndCheckInterrupt() {
      blocked = Thread.currentThread();
      LockSupport.park(this);
      return Thread.interrupted();
    }
  }

  private static final MethodDescriptor<QueryRequest, DefaultMetaTimeSeriesQueryResult>
      METHOD_GET_TIMESERIES =
          MystServiceGrpc.getGetTimeseriesMethod().toBuilder(
                  ProtoLiteUtils.marshaller(QueryRequest.getDefaultInstance()),
                  TimeSeriesQueryResultMarshaller.marshaller())
              .build();

  private static final class MetaResultAsyncSub
      extends io.grpc.stub.AbstractAsyncStub<MetaResultAsyncSub> {

    protected MetaResultAsyncSub(Channel channel) {
      super(channel, CallOptions.DEFAULT.withExecutor(Executors.newFixedThreadPool(10)));
    }

    protected MetaResultAsyncSub(Channel channel, CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected MetaResultAsyncSub build(Channel channel, CallOptions callOptions) {
      return new MetaResultAsyncSub(channel, callOptions);
    }

    public void getTimeseries(
        QueryRequest request,
        io.grpc.stub.StreamObserver<DefaultMetaTimeSeriesQueryResult> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_TIMESERIES, getCallOptions()), request, responseObserver);
    }
  }

  private static final class MetaResultBlockingStub
      extends AbstractBlockingStub<MetaResultBlockingStub> {

    protected MetaResultBlockingStub(Channel channel) {
      super(channel, CallOptions.DEFAULT);
    }

    protected MetaResultBlockingStub(Channel channel, CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected MetaResultBlockingStub build(Channel channel, CallOptions callOptions) {
      return new MetaResultBlockingStub(channel, callOptions);
    }

    public Iterator<DefaultMetaTimeSeriesQueryResult> getTimeseries(QueryRequest request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_GET_TIMESERIES, getCallOptions(), request);
    }
  }
}
