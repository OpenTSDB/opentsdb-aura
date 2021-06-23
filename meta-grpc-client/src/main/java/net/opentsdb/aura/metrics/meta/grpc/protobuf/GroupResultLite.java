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

package net.opentsdb.aura.metrics.meta.grpc.protobuf;


import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;

import java.io.OutputStream;

public class GroupResultLite extends DefaultMetaTimeSeriesQueryResult.DefaultGroupResult implements MessageLite {
  @Override
  public void writeTo(CodedOutputStream output) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getSerializedSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Parser<? extends MessageLite> getParserForType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteString toByteString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] toByteArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeTo(OutputStream output) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeDelimitedTo(OutputStream output) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Builder newBuilderForType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Builder toBuilder() {
    throw new UnsupportedOperationException();
  }

  @Override
  public MessageLite getDefaultInstanceForType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isInitialized() {
    throw new UnsupportedOperationException();
  }

}
