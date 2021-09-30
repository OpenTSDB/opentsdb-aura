package net.opentsdb.aura.metrics.meta.endpoints.impl;

import net.opentsdb.aura.metrics.meta.endpoints.ShardedServiceRegistry;

//TODO:
// Parse the yaml file.
// Look at Plugin implementation.
// if it is too complicated, then parse the file yourself.
// Parsing the file should use ObjectMapper.
public abstract class KubernetesStatefulRegistry implements ShardedServiceRegistry {

    private String type;
    private final String[] replicas = {"a", "b", "c", "d", "e"};
    protected static final String NUM_SHARDS = "num-shards";
    protected static final String REPLICAS = "replicas";
    protected static final String K8S_NAMESPACE = "k8s_namespace";

    public KubernetesStatefulRegistry(final String type) {
        this.type = type;
    }

    protected int getNumShards(String namespace) {
        return 0;
    }

    protected int getNumReplicas(String namespace) {
        return 0;
    }

    protected String getReplica(int replicaId) {
        return replicas[replicaId];
    }

    protected int getPort(String namespace) {
        return 0;
    }

    protected String getDomain(String namespace) {
        return null;
    }
}
