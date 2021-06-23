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

import com.google.protobuf.AbstractParser;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;

public class GroupResultParser extends AbstractParser<GroupResultLite> {
  @Override
  public GroupResultLite parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {

    try {
      GroupResultLite groupResult = new GroupResultLite();
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 8: {
            groupResult.addTagHash(input.readInt64());
            break;
          }
          case 10: {
            int length = input.readRawVarint32();
            int limit = input.pushLimit(length);
            while (input.getBytesUntilLimit() > 0) {
              groupResult.addTagHash(input.readInt64());
            }
            input.popLimit(limit);
            break;
          }
          case 16: {
            groupResult.addHash(input.readInt64());
            break;
          }
          case 18: {
            int length = input.readRawVarint32();
            int limit = input.pushLimit(length);
            while (input.getBytesUntilLimit() > 0) {
              groupResult.addHash(input.readInt64());
            }
            input.popLimit(limit);
            break;
          }
          default: {
            if (!input.skipField(tag)) {
              done = true;
            }
            break;
          }
        }
      }
      return groupResult;
    } catch (IOException e) {
      throw new InvalidProtocolBufferException(e);
    }
  }
}
