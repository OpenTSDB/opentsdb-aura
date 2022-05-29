package net.opentsdb.aura.metrics.meta.endpoints;

import java.util.List;
import java.util.Map;

public interface ShardedServiceRegistry {

    Map<String, List<ShardEndPoint>> getEndpoints(String namespace);

    Map<String, List<ShardEndPoint>> getEndpoints(String namespace, long epoch);
}
