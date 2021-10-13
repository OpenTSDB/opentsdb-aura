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

package net.opentsdb.aura.metrics.meta.endpoints.impl;

import net.opentsdb.aura.metrics.meta.endpoints.ShardEndPoint;
import net.opentsdb.aura.metrics.meta.endpoints.ShardedServiceRegistry;

import java.util.*;


public class MystStatefulSetRegistry implements ShardedServiceRegistry {

    private static final String pod_name_pattern = "%s-myst-%s-%s.%s.svc.%s";

    private DeploymentConfig config;

    public MystStatefulSetRegistry(DeploymentConfig config) {
        this.config=config;
    }

    public MystStatefulSetRegistry() {
    }

    @Override
    public Map<String, List<ShardEndPoint>> getEndpoints(String namespace) {
        Map<String, List<ShardEndPoint>> endpointsMap = new HashMap<>();

        final int numShards = config.toMap().get(namespace).toMap().get("myst").getNumShards();
        final int numReplicas = config.toMap().get(namespace).toMap().get("myst").getNumReplicas();
        for(int i = 0; i < numReplicas; i++) {
            String replica = config.toMap().get(namespace).toMap().get("myst").getReplica(i);
            List<ShardEndPoint> shardEndPoints = new ArrayList<>();
            endpointsMap.put(replica, shardEndPoints);
            for (int j = 0; j < numShards; j++) {
                final MystEndpoint endpoint = MystEndpoint.Builder.newBuilder()
                        .withHost(
                                String.format(
                                        pod_name_pattern,
                                        namespace,
                                        replica,
                                        j,
                                        namespace,
                                        config.getClusterDomain()))
                        .withPort(config.toMap().get(namespace).toMap().get("myst").getPort())
                        .build();
                shardEndPoints.add(endpoint);

            }
        }
        return endpointsMap;
    }

    @Override
    public Map<String, List<ShardEndPoint>> getEndpoints(String namespace, long epoch) {
        return getEndpoints(namespace);
    }
}
