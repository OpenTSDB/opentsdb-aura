package net.opentsdb.aura.metrics.meta;

import java.util.Iterator;

public interface ShardedMetaClient<ResT extends MetaTimeSeriesQueryResult> {

    Iterator<ResT> getTimeseriesAllShards(String namespace, String query) throws MetaFetchException;

    ResT getTimeSeriesPerShard(String namespace, String query, int shardId) throws MetaFetchException;

}
