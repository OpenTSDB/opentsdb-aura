package com.yahoo.yamas.aura.metrics.meta.grpc;

import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MystClientCall<ReqT, RespT>
    extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MystClientCall.class);

  protected MystClientCall(ClientCall<ReqT, RespT> delegate) {
    super(delegate);
  }

  @Override
  public void start(Listener<RespT> responseListener, Metadata headers) {
    LOGGER.trace("Call started at: {}", System.nanoTime());
    super.start(new MystClientCallListener<>(responseListener), headers);
  }

}
