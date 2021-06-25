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
