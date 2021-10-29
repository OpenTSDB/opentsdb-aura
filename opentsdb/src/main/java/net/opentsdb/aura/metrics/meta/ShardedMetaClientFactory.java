package net.opentsdb.aura.metrics.meta;

public interface ShardedMetaClientFactory<ResT extends MetaTimeSeriesQueryResult> {
    ShardedMetaClient<ResT> getShardedMetaClient(String namespace);
}
