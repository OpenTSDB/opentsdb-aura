package net.opentsdb.aura.metrics.meta.endpoints.impl;

import net.opentsdb.aura.metrics.meta.endpoints.ShardEndPoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//TODO:
// make a cache
// Refetch for a namespace only if things have changed
// Write testcases using Mocks (jmockit or mockito)
public class MystStatefulSetRegistry extends KubernetesStatefulRegistry {

    private static final String pod_name_pattern = "%s-myst-%s-%s.svc.%s";

    public MystStatefulSetRegistry() {
        super("myst");
    }

    @Override
    public Map<String, List<ShardEndPoint>> getEndpoints(String namespace) {
        Map<String, List<ShardEndPoint>> endpointsMap = new HashMap<>();

        final int numShards = getNumShards(namespace);
        final int numReplicas = getNumReplicas(namespace);
        for(int i = 0; i < numReplicas; i++) {
            String replica = getReplica(i);
            List<ShardEndPoint> shardEndPoints = new ArrayList<>();
            endpointsMap.put(replica, shardEndPoints);
            for (int j = 0; j < numShards; i++) {
                final MystEndpoint endpoint = MystEndpoint.Builder.newBuilder()
                        .withHost(
                                String.format(
                                        pod_name_pattern,
                                        namespace,
                                        replica,
                                        j,
                                        getDomain(namespace)))
                        .withPort(getPort(namespace))
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
