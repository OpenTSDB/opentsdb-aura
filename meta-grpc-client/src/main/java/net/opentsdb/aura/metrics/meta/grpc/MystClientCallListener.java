package com.yahoo.yamas.aura.metrics.meta.grpc;

import io.grpc.ClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MystClientCallListener<RespT> extends ForwardingClientCallListener<RespT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MystClientCall.class);

  private ClientCall.Listener<RespT> delegate;

  public MystClientCallListener(ClientCall.Listener<RespT> delegate) {
    this.delegate = delegate;
  }

  @Override
  protected ClientCall.Listener<RespT> delegate() {
    return delegate;
  }

  @Override
  public void onMessage(RespT message) {
    LOGGER.trace("Message received at {}", System.nanoTime());
    super.onMessage(message);
  }

  @Override
  public void onHeaders(Metadata headers) {
    LOGGER.trace("Headers received at {}", System.nanoTime());
    super.onHeaders(headers);
  }

  @Override
  public void onClose(Status status, Metadata trailers) {
    LOGGER.trace("Call closed at {}", System.nanoTime());
    super.onClose(status, trailers);
  }
}
