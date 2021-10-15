package net.opentsdb.aura.metrics.meta;

import java.util.Iterator;

public interface ShardedMetaClient<ResT extends MetaTimeSeriesQueryResult> {

    Iterator<ResT> getTimeseriesAllShards(String namespace, String query);

    ResT getTimeSeriesPerShard(String namespace, String query, int shardId);

}
