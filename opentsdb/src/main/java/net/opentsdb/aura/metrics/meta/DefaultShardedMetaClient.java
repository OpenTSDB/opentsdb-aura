package net.opentsdb.aura.metrics.meta;

import com.stumbleupon.async.Deferred;
import net.opentsdb.aura.metrics.meta.endpoints.ShardEndPoint;
import net.opentsdb.aura.metrics.meta.endpoints.ShardedServiceRegistry;
import net.opentsdb.aura.metrics.meta.grpc.MetaGrpcClient;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

import java.util.*;
import java.util.concurrent.*;

public class DefaultShardedMetaClient
        extends BaseTSDBPlugin
        implements ShardedMetaClient<MergedMetaTimeSeriesQueryResult>  {

    public static final String TYPE = "SerialShardedMetaClient";

    private volatile ShardedServiceRegistry registry;

    private volatile ExecutorService service;

    public static final String META_SERVICE_REGISTRY = "meta.service.registry";
    public static final String THREAD_POOL_SIZE = "default.sharded.meta.client.pool.size";

    private Map<ShardEndPoint, MetaGrpcClient> clients = new ConcurrentHashMap<>();
    private final String MUTEX = new String();
    private Random random = new Random();

    private List<ShardEndPoint> setUpEndpoints(String namespace) {
        final Map<String, List<ShardEndPoint>> endpointsMap = registry.getEndpoints(namespace);
        List<ShardEndPoint> endPoints =  getEndpoints(endpointsMap);

        createClients(endPoints);
        return endPoints;
    }

    /**
     * Naive parallelization.
     * Better semantics maybe achieved by using a blocking queue.
     * TODO: Object re-use - by using object pools ?
     * Has to be ThreadSafe
     * @param namespace
     * @param query
     * @return
     * @throws MetaFetchException
     */
    @Override
    public Iterator<MergedMetaTimeSeriesQueryResult> getTimeseriesAllShards(String namespace, String query) throws MetaFetchException {

        final List<ShardEndPoint> shardEndPoints = setUpEndpoints(namespace);
        List<MergedMetaTimeSeriesQueryResult> timeSeriesQueryResults = new ArrayList<>();
        final List<Future<MergedMetaTimeSeriesQueryResult>> futures = new ArrayList<>();
        for (ShardEndPoint endPoint : shardEndPoints) {
            futures.add(getResult(clients.get(endPoint), query));
        }

        for(int i = 0 ; i < futures.size(); i++) {
            try {
                timeSeriesQueryResults.add(futures.get(i).get());
            } catch (InterruptedException e) {
                throw new RuntimeException("Meta thread interrupted when fetching", e);
            } catch (ExecutionException e) {
                throw new MetaFetchException("Meta fetch failure for: " + i, e);
            }
        }

        return timeSeriesQueryResults.iterator();
    }

    private Future<MergedMetaTimeSeriesQueryResult> getResult(MetaGrpcClient metaGrpcClient, String query) {
        return this.service.submit(new MetaCall(metaGrpcClient, query, new MergedMetaTimeSeriesQueryResult()));
    }

    private static class MetaCall implements Callable<MergedMetaTimeSeriesQueryResult> {

        private final MetaGrpcClient metaGrpcClient;
        private final String query;
        private final MergedMetaTimeSeriesQueryResult mergedMetaTimeSeriesQueryResult;

        public MetaCall(MetaGrpcClient metaGrpcClient, String query, MergedMetaTimeSeriesQueryResult mergedMetaTimeSeriesQueryResult) {

            this.metaGrpcClient = metaGrpcClient;
            this.query = query;
            this.mergedMetaTimeSeriesQueryResult = mergedMetaTimeSeriesQueryResult;
        }

        @Override
        public MergedMetaTimeSeriesQueryResult call() throws Exception {
            final Iterator<DefaultMetaTimeSeriesQueryResult> timeseries = metaGrpcClient.getTimeseries(query);
            while (timeseries.hasNext()) {
                mergedMetaTimeSeriesQueryResult.add(timeseries.next());
            }
            return mergedMetaTimeSeriesQueryResult;
        }
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
    public MergedMetaTimeSeriesQueryResult getTimeSeriesPerShard(String namespace, String query, int shardId) throws MetaFetchException {

        final ShardEndPoint shardEndPoint = setUpEndpoints(namespace).get(shardId);
        try {
            return getResult(clients.get(shardEndPoint), query).get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Meta thread interrupted when fetching", e);
        } catch (ExecutionException e) {
            throw new MetaFetchException("Meta fetch failure", e);
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Deferred<Object> initialize(TSDB tsdb, String id) {

        final Configuration config = tsdb.getConfig();

        registry = config.getTyped(META_SERVICE_REGISTRY, ShardedServiceRegistry.class);

        config.register(THREAD_POOL_SIZE, 10, false,
                "Thread pool size for sharded meta client");

        final int anInt = config.getInt(THREAD_POOL_SIZE);
        service = Executors.newWorkStealingPool(anInt);
        return null;
    }
}
