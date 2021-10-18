package net.opentsdb.aura.metrics.meta.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.metrics.meta.endpoints.impl.Component;
import net.opentsdb.aura.metrics.meta.endpoints.impl.DeploymentConfig;
import net.opentsdb.aura.metrics.meta.endpoints.impl.Namespace;
import net.opentsdb.aura.metrics.meta.endpoints.impl.SimpleEndPoint;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BaseStatefulSetRegistry extends BaseTSDBPlugin implements ShardedServiceRegistry {

    public static final String TYPE = "BaseStatefulSetRegistry";

    public static final String DOMAIN = "statefulset.domain";

    public static final String DEFAULT_NAMESPACE = "statefulset.default.namespace";

    public static final String DEPLOYMENT_CONFIG = "statefulset.namespaces";

    private static final String pod_name_pattern = "%s-%s.%s.svc.%s";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private volatile Map<String, Map<String, List<ShardEndPoint>>> endPoints = new ConcurrentHashMap<>();

    private volatile DeploymentConfig deploymentConfig;

    @Override
    public Map<String, List<ShardEndPoint>> getEndpoints(String namespace) {
        return getEndpoints(namespace, -1);
    }


    public Map<String, List<ShardEndPoint>> getEndpoints(String namespace, long epoch) {
        if (endPoints.get(namespace) != null) {
            return endPoints.get(namespace);
        } else {
            return getAndCacheEndPoints(namespace, epoch);
        }
    }

    private Map<String, List<ShardEndPoint>> getAndCacheEndPoints(String namespace, long epoch) {

        final Map<String, List<ShardEndPoint>> endpoints = getEndpoints(namespace, deploymentConfig, epoch);

        this.endPoints.put(namespace, endpoints);

        return endpoints;
    }

    public Map<String, List<ShardEndPoint>> getEndpoints(String namespace, DeploymentConfig deploymentConfig, long epoch) {

        final Namespace namespace1 = deploymentConfig.getNamespace(namespace);
        final String clusterDomain = deploymentConfig.getClusterDomain();

        final Component component = namespace1.getComponent(getComponentName());

        final int numReplicas = component.getNumReplicas();
        final int numShards = component.getNumShards();

        Map<String, List<ShardEndPoint>> endpointsMap = new HashMap<>();

        for(int i = 0; i < numReplicas; i++) {

            String replica = component.getReplica(i);
            List<ShardEndPoint> shardEndPoints = new ArrayList<>();
            endpointsMap.put(replica, shardEndPoints);
            final String prefix = getPrefix(namespace, component, replica, epoch);
            for (int j = 0; j < numShards; j++) {
                final SimpleEndPoint endpoint = SimpleEndPoint.Builder.newBuilder()
                        .withHost(
                                String.format(
                                        pod_name_pattern,
                                        prefix,
                                        j,
                                        prefix,
                                        clusterDomain))
                        .withPort(component.getPort())
                        .build();
                shardEndPoints.add(endpoint);
            }
        }

        return endpointsMap;

    }

    protected abstract String getComponentName();

    protected abstract String getPrefix(String namespace, Component component, String replica, long epoch);

    @Override
    public abstract String type();

    @Override
    public Deferred<Object> initialize(TSDB tsdb, String id) {
        final Configuration config = tsdb.getConfig();

        try {
            final Namespace[] namespaces = objectMapper.readValue(config.getString(DEPLOYMENT_CONFIG).getBytes(), Namespace[].class);
            this.deploymentConfig = DeploymentConfig.Builder.newBuilder()
                    .withClusterDomain(config.getString(DOMAIN))
                    .withDefaultNamespace(config.getString(DEFAULT_NAMESPACE))
                    .withNamespaces(namespaces)
                    .build();
            return null;
        } catch (IOException e) {
            throw new RuntimeException( "Error deserializing deployment config for stateful set", e);
        }
    }

}
