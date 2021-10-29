package net.opentsdb.aura.metrics.meta;

public class DefaultShardedMetaClientFactory implements ShardedMetaClientFactory<MergedMetaTimeSeriesQueryResult> {
    @Override
    public ShardedMetaClient<MergedMetaTimeSeriesQueryResult> getShardedMetaClient(String namespace) {
        return new DefaultShardedMetaClient();
    }
}
