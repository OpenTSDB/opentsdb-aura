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
import io.grpc.MethodDescriptor;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import net.opentsdb.aura.metrics.meta.grpc.protobuf.BitmapGroupResultLite;
import net.opentsdb.aura.metrics.meta.grpc.protobuf.BitmapGroupResultParser;
import net.opentsdb.aura.metrics.meta.grpc.protobuf.DictionaryLite;
import net.opentsdb.aura.metrics.meta.grpc.protobuf.DictionaryParser;

import java.io.IOException;
import java.io.InputStream;

public class TimeSeriesQueryResultMarshaller {

  private static final BitmapGroupResultParser groupedTsParser = new BitmapGroupResultParser();
  private static final DictionaryParser dictParser = new DictionaryParser();

  private TimeSeriesQueryResultMarshaller() {}

//  public static long parseTime;
//  public static long size;

  public static MethodDescriptor.Marshaller<DefaultMetaTimeSeriesQueryResult> marshaller() {

    return new MethodDescriptor.Marshaller<DefaultMetaTimeSeriesQueryResult>() {

      @Override
      public InputStream stream(DefaultMetaTimeSeriesQueryResult result) {
        throw new UnsupportedOperationException();
      }

      @Override
      public DefaultMetaTimeSeriesQueryResult parse(InputStream stream) {
//        long start = System.nanoTime();
        DefaultMetaTimeSeriesQueryResult result =
            new DefaultMetaTimeSeriesQueryResult(); // TODO: reuse
        ExtensionRegistryLite extensionRegistry = ExtensionRegistryLite.newInstance();
        try {
//          size += stream.available();
          CodedInputStream input = CodedInputStream.newInstance(stream); // TODO: source of garbage

          try {
            boolean done = false;
            while (!done) {
              int tag = input.readTag();
              switch (tag) {
                case 0:
                  done = true;
                  break;
                case 10:
                  {
                    BitmapGroupResultLite gt =
                        input.readMessage(groupedTsParser, extensionRegistry);
                    result.addGroupResult(gt);
                    break;
                  }
                case 18:
                  {
                    DictionaryLite dictionary = input.readMessage(dictParser, extensionRegistry);
                    result.setDictionary(dictionary);
                    break;
                  }
                case 24:
                  {
                    result.setTotalResults(input.readInt32());
                    break;
                  }
                default:
                  {
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
//        long time = System.nanoTime() - start;
//        parseTime += time;
        return result;
      }
    };
  }
}
