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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.grpc.protobuf.DictionaryLite;
import net.opentsdb.aura.metrics.meta.grpc.protobuf.DictionaryParser;
import net.opentsdb.aura.metrics.meta.grpc.protobuf.GroupResultLite;
import net.opentsdb.aura.metrics.meta.grpc.protobuf.GroupResultParser;
import io.grpc.KnownLength;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

public class TimeSeriesQueryResultMarshaller {

  private static final ThreadLocal<Reference<byte[]>> bufs = new ThreadLocal<>();
  private static final GroupResultParser groupedTsParser = new GroupResultParser();
  private static final DictionaryParser dictParser = new DictionaryParser();


  private TimeSeriesQueryResultMarshaller() {
  }

  public static MethodDescriptor.Marshaller<DefaultMetaTimeSeriesQueryResult> marshaller() {

    return new MethodDescriptor.Marshaller<DefaultMetaTimeSeriesQueryResult>() {

      @Override
      public InputStream stream(DefaultMetaTimeSeriesQueryResult result) {
        throw new UnsupportedOperationException();
      }

      @Override
      public DefaultMetaTimeSeriesQueryResult parse(InputStream stream) {

        DefaultMetaTimeSeriesQueryResult result = new DefaultMetaTimeSeriesQueryResult(); //TODO: reuse
        ExtensionRegistryLite extensionRegistry = ExtensionRegistryLite.newInstance();
        if (stream instanceof KnownLength) {
          try {
            int size = stream.available();
            Reference<byte[]> ref;
            byte[] buf;
            if ((ref = bufs.get()) == null || (buf = ref.get()) == null || buf.length < size) {
              buf = new byte[size];
              bufs.set(new WeakReference<>(buf));
            }

            int remaining = size;
            while (remaining > 0) {
              int position = size - remaining;
              int count = stream.read(buf, position, remaining);
              if (count == -1) {
                break;
              }
              remaining -= count;
            }

            if (remaining != 0) {
              int position = size - remaining;
              throw new RuntimeException("size inaccurate: " + size + " != " + position);
            }

            CodedInputStream input = CodedInputStream.newInstance(buf, 0, size); // TODO: source of garbage

            try {
              boolean done = false;
              while (!done) {
                int tag = input.readTag();
                switch (tag) {
                  case 0:
                    done = true;
                    break;
                  case 10: {
                    GroupResultLite gt = input.readMessage(groupedTsParser, extensionRegistry);
                    result.addGroupResult(gt);
                    break;
                  }
                  case 18: {
//                    dictParser.parsePartialFrom(input, response.dict);
                    DictionaryLite dictionary = input.readMessage(dictParser, extensionRegistry);
                    result.setDictionary(dictionary);
                    break;
                  }
                  case 24: {
                    result.setTotalResults(input.readInt32());
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
              throw new com.google.protobuf.InvalidProtocolBufferException(e);
            }

          } catch (IOException e) {
            result.setException(e);
          }
        }
        return result;
      }
    };

  }
}