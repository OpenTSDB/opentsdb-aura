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
import net.opentsdb.aura.metrics.meta.MetaClient;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.StreamObserver;
import myst.MystServiceGrpc;
import myst.QueryRequest;

import java.util.Iterator;

import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;

public class MetaGrpcClient implements MetaClient<DefaultMetaTimeSeriesQueryResult> {

  private final ManagedChannel channel;
  private final MetaResultBlockingStub blockingStub;
  private final MetaResultAsyncSub asyncSub;

  public MetaGrpcClient(final String host, final int port) {
    this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().enableRetry().maxRetryAttempts(3).build();
    this.blockingStub = new MetaResultBlockingStub(channel);
    this.asyncSub = new MetaResultAsyncSub(channel);
  }

  @Override
  public Iterator<DefaultMetaTimeSeriesQueryResult> getTimeseries(String query) {
    QueryRequest queryRequest = QueryRequest.newBuilder().setQuery(query).build();
    return blockingStub.getTimeseries(queryRequest);
  }

  @Override
  public void getTimeseries(String query, StreamConsumer<DefaultMetaTimeSeriesQueryResult> consumer) {
        QueryRequest queryRequest = QueryRequest.newBuilder().setQuery(query).build();
    asyncSub.getTimeseries(queryRequest, new StreamObserver<DefaultMetaTimeSeriesQueryResult>() {

      @Override
      public void onNext(DefaultMetaTimeSeriesQueryResult result) {
        consumer.onNext(result);
      }

      @Override
      public void onError(Throwable t) {
        consumer.onError(t);
      }

      @Override
      public void onCompleted() {
        consumer.onCompleted();
      }
    });
  }


  private static final MethodDescriptor<QueryRequest, DefaultMetaTimeSeriesQueryResult> METHOD_GET_TIMESERIES = MystServiceGrpc.getGetTimeseriesMethod()
      .toBuilder(
          ProtoLiteUtils.marshaller(QueryRequest.getDefaultInstance()),
          TimeSeriesQueryResultMarshaller.marshaller())
      .build();

  private static final class MetaResultAsyncSub extends io.grpc.stub.AbstractAsyncStub<MetaResultAsyncSub> {

    protected MetaResultAsyncSub(Channel channel) {
      super(channel, CallOptions.DEFAULT);
    }

    protected MetaResultAsyncSub(Channel channel, CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected MetaResultAsyncSub build(Channel channel, CallOptions callOptions) {
      return new MetaResultAsyncSub(channel, callOptions);
    }

    public void getTimeseries(QueryRequest request,
                              io.grpc.stub.StreamObserver<DefaultMetaTimeSeriesQueryResult> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_GET_TIMESERIES, getCallOptions()), request, responseObserver);
    }
  }

  private static final class MetaResultBlockingStub extends AbstractBlockingStub<MetaResultBlockingStub> {

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
