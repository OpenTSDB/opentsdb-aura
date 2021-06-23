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

import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import myst.MystServiceGrpc;
import myst.QueryRequest;
import myst.TimeseriesResponse;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult.GroupResult;
import net.opentsdb.aura.metrics.meta.MetaTimeSeriesQueryResult.GroupResult.TagHashes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetaGrpcClientFunctionalTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetaGrpcClientFunctionalTest.class);

  String host = "10.214.168.140";
  int port = 9999;

  private MetaGrpcClient client = new MetaGrpcClient(host, port);

//  private String queryString = "{\"from\":0,\"to\":1,\"start\":1616508000,\"end\":1616509800,\"order\":\"ASCENDING\",\"type\":\"TIMESERIES\",\"limit\":100,\"group\":[\"flid\"],\"namespace\":\"Yamas\",\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":\"TagValueRegex\",\"key\":\"flid\",\"filter\":\"1111\"},{\"type\":\"TagValueRegex\",\"key\":\"node_function_type\",\"filter\":\"normal\"},{\"type\":\"MetricLiteral\",\"metric\":\"exch.bdr.Bids\"}]}}";
  private String queryString = "{\"from\":0,\"to\":1,\"start\":1616508000,\"end\":1616509800,\"order\":\"ASCENDING\",\"type\":\"TIMESERIES\",\"limit\":100,\"group\":[\"flid\"],\"namespace\":\"Yamas\",\"filter\":{\"type\":\"Chain\",\"filters\":[{\"type\":\"Chain\",\"op\":\"OR\",\"filters\":[{\"type\":\"TagValueLiteralOr\",\"filter\":\"ats-proxy|ats\",\"key\":\"corp:Application\"}]},{\"type\":\"TagValueLiteralOr\",\"filter\":\"prod\",\"key\":\"corp:Environment\"},{\"type\":\"TagValueRegex\",\"filter\":\".*\",\"key\":\"InstanceId\"},{\"type\":\"MetricLiteral\",\"metric\":\"net.bytes_sent\"}]}}";

  @Test
  @Disabled
  void remoteCall() {
    Iterator<DefaultMetaTimeSeriesQueryResult> timeseriesItr = client.getTimeseries(queryString);
    int count = 0;
    while (timeseriesItr.hasNext()) {
      DefaultMetaTimeSeriesQueryResult result = timeseriesItr.next();

      int numGroups = result.numGroups();
      for (int j = 0; j < numGroups; j++) {
        GroupResult group = result.getGroup(j);

        TagHashes tagHashes = group.tagHashes();
        int tagHashCount = tagHashes.size();
        LOGGER.info("tag hash count: " + tagHashCount);

        for (int i = 0; i < tagHashCount; i++) {
          long tagHash = tagHashes.next();
          assertTrue(tagHash != 0);
          assertNotNull(result.getStringForHash(tagHash));
        }

        int hashCount = group.numHashes();
        LOGGER.info("hash count: " + hashCount);
        for (int i = 0; i < hashCount; i++) {
          assertTrue(group.getHash(i) != 0);
        }
      }
      count++;
    }
    assertTrue(count > 0);
  }

  @Test
  @Disabled
  void callWithDefaultClient() {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    MystServiceGrpc.MystServiceBlockingStub blockingStub = MystServiceGrpc.newBlockingStub(channel);

    QueryRequest query = QueryRequest.newBuilder().setQuery(queryString).build();
    Iterator<TimeseriesResponse> responseItr = blockingStub.getTimeseries(query);
    int i = 1;
    while (responseItr.hasNext()) {
      TimeseriesResponse response = responseItr.next();
      System.out.println(i++);
      System.out.println(response.toString());
    }
  }
}
