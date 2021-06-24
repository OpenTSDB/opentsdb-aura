package com.yahoo.yamas.aura.metrics.meta.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;

public class MystClientInterceptor implements io.grpc.ClientInterceptor {

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new MystClientCall<>(next.newCall(method, callOptions));
  }
}
