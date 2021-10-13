package net.opentsdb.aura.metrics.meta;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

import java.util.Iterator;

public class SerialShardedMetaClient extends BaseTSDBPlugin implements ShardedMetaClient {

    public static final String TYPE = "ShardedMetaClient";

    @Override
    public Iterator<Iterator> getTimeseriesAllShards() {
        return null;
    }

    @Override
    public Iterator getTimeSeriesPerShard(int shardId) {
        return null;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Deferred<Object> initialize(TSDB tsdb, String id) {



        return null;
    }
}
