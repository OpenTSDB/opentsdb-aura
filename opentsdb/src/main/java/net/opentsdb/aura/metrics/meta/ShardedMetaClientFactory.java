package net.opentsdb.aura.metrics.meta;

public interface ShardedMetaClientFactory {

    ShardedMetaClient getShardedMetaClient(String namespace);

}
