package net.opentsdb.aura.metrics.meta;

import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.metrics.meta.endpoints.ShardEndPoint;
import net.opentsdb.aura.metrics.meta.endpoints.ShardedServiceRegistry;
import net.opentsdb.aura.metrics.meta.grpc.MetaGrpcClient;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultShardedMetaClient
        extends BaseTSDBPlugin
        implements ShardedMetaClient<MergedMetaTimeSeriesQueryResult>  {

    public static final String TYPE = "SerialShardedMetaClient";

    private volatile ShardedServiceRegistry registry;

    private Map<ShardEndPoint, MetaGrpcClient> clients = new ConcurrentHashMap<>();
    private final String MUTEX = new String();
    private Random random = new Random();

    private List<ShardEndPoint> setUpEndpoints(String namespace) {
        final Map<String, List<ShardEndPoint>> endpointsMap = registry.getEndpoints(namespace);
        List<ShardEndPoint> endPoints =  getEndpoints(endpointsMap);

        createClients(endPoints);
        return endPoints;
    }

    @Override
    public Iterator<MergedMetaTimeSeriesQueryResult> getTimeseriesAllShards(String namespace, String query) {

        final List<ShardEndPoint> shardEndPoints = setUpEndpoints(namespace);
        List<MergedMetaTimeSeriesQueryResult> timeSeriesQueryResults = new ArrayList<>();
        for (ShardEndPoint endPoint : shardEndPoints) {
            timeSeriesQueryResults.add(getResult(clients.get(endPoint), query));
        }

        return timeSeriesQueryResults.iterator();
    }

    private MergedMetaTimeSeriesQueryResult getResult(MetaGrpcClient metaGrpcClient, String query) {
        MergedMetaTimeSeriesQueryResult mergedMetaTimeSeriesQueryResult = new MergedMetaTimeSeriesQueryResult();
        final Iterator<DefaultMetaTimeSeriesQueryResult> timeseries = metaGrpcClient.getTimeseries(query);
        while (timeseries.hasNext()) {
            mergedMetaTimeSeriesQueryResult.add(timeseries.next());
        }
        return mergedMetaTimeSeriesQueryResult;
    }

    private void createClients(List<ShardEndPoint> endPoints) {

        for (ShardEndPoint endPoint : endPoints) {
            if (!this.clients.containsKey(endPoint)) {
                synchronized (MUTEX) {
                    if (!clients.containsKey(endPoint)) {
                        clients.put(endPoint, new MetaGrpcClient(endPoint.getHost(), endPoint.getPort()));
                    }
                }
            }
        }
    }

    private List<ShardEndPoint> getEndpoints(Map<String, List<ShardEndPoint>> endpointsMap) {
        final int i = random.nextInt(endpointsMap.size());
        int j = 0;
        for(String key: endpointsMap.keySet()) {
            if (j == i) {
                return endpointsMap.get(key);
            }
            j++;
        }

        return null;
    }

    @Override
    public MergedMetaTimeSeriesQueryResult getTimeSeriesPerShard(String namespace, String query, int shardId) {

        final ShardEndPoint shardEndPoint = setUpEndpoints(namespace).get(shardId);
        return getResult(clients.get(shardEndPoint), query);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Deferred<Object> initialize(TSDB tsdb, String id) {

        final Configuration config = tsdb.getConfig();

        registry = config.getTyped("meta.service.registry", ShardedServiceRegistry.class);

        return null;
    }
}
