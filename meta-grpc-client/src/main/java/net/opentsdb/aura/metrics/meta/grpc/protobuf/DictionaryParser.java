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

public class DictionaryParser extends AbstractParser<DictionaryLite> {

  private static long NOT_SET = Long.MIN_VALUE;

  @Override
  public DictionaryLite parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
    DictionaryLite dictionary = new DictionaryLite();
    parsePartialFrom(input, dictionary);
    return dictionary;
  }

  public void parsePartialFrom(CodedInputStream input, DictionaryLite dictionary) throws InvalidProtocolBufferException {

    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 10: {
            // parse entry
            input.readRawVarint32(); //ignore
            long id = NOT_SET;
            while (true) {
              int t = input.readTag();
              if (t == 0) {
                break;
              }
              if (t == 8) {
                id = input.readInt64();
              } else if (t == 18) {
                if (id == NOT_SET) {
                  throw new InvalidProtocolBufferException("id not found for dictionary entry");
                }
                final int size = input.readRawVarint32();
                byte[] value = input.readRawBytes(size); // TODO: source of garbage
                dictionary.put(id, value);
                break;
              } else {
                if (!input.skipField(t)) {
                  break;
                }
              }
            }
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
    } catch (java.io.IOException e) {
      throw new InvalidProtocolBufferException(e);
    }

  }
}
