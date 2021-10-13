package net.opentsdb.aura.metrics.meta;

import java.util.Iterator;

public interface ShardedMetaClient<ResT extends MetaTimeSeriesQueryResult> {

    Iterator<Iterator<ResT>> getTimeseriesAllShards();

    Iterator<ResT> getTimeSeriesPerShard(int shardId);

}
