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
