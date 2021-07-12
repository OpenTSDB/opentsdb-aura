/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  Yahoo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.aura.metrics.storage;

import net.opentsdb.aura.metrics.core.TimeSeriesShardIF;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.XXHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some low level sources may contain multiple time series that would hash to
 * different shards. For such cases we need to stash the data in a container that
 * contains only data destined for that shard.
 *
 * TODO - find a more efficient way to do this.
 */
public class LowLevelMetricShardContainer implements LowLevelMetricData.HashedLowLevelMetricData,
        Runnable,
        CloseablePooledObject {
    private static Logger logger = LoggerFactory.getLogger(LowLevelMetricShardContainer.class);

    /** The pooled object ref. */
    protected PooledObject pooled_object;

    // 4b length of message, 8b timestamp, 8b tags hash, 4b tags length, xb tags,
    // 8b series hash, 4b metric len, xb metric, 8b double val. REPEAT if more time series for
    // this shard
    protected byte[] buffer = new byte[4096];

    // write fields
    protected long last_tags_hash;
    protected int buffer_end;
    protected int current_event_length_idx;
    public TimeSeriesShardIF shard;

    // read fields
    protected int buffer_offset;
    protected int cur_length; // of the entire tag and metric set
    protected int cur_idx;    // current read pointer.
    protected int cur_read;   // how much we've read so far of the current event.
    protected int tags_start;
    protected int tags_length;
    protected int tags_count;
    protected int metric_start;
    protected int metric_length;
    protected long metric_hash;
    protected int[] tag_indices = new int[16];
    protected int tag_idx;
    protected int cur_tag_idx;
    protected long timestamp;
    protected long series_hash;
    protected long tags_hash;
    double value;

    // write methods
    public void append(LowLevelMetricData.HashedLowLevelMetricData data) {
        LowLevelMetricData.HashedLowLevelMetricData hashed = (LowLevelMetricData.HashedLowLevelMetricData) data;
        if (last_tags_hash == hashed.tagsSetHash()) {
            require(8 + 4 + hashed.metricLength() + 8);
            writeMetric(hashed);
        } else {
            // flush the previous
            Bytes.setInt(buffer, buffer_end - current_event_length_idx - 4, current_event_length_idx);
            current_event_length_idx = buffer_end;

            require(4 + 8 + 8 + 4 + hashed.tagBufferLength() + 8 + 4 + hashed.metricLength() + 8);

            // skip length
            buffer_end += 4;

            // just doing seconds for now
            Bytes.setLong(buffer, hashed.timestamp().epoch(), buffer_end);
            buffer_end += 8;

            // tag set hash
            Bytes.setLong(buffer, hashed.tagsSetHash(), buffer_end);
            buffer_end += 8;

            // tag buffer length
            Bytes.setInt(buffer, hashed.tagBufferLength(), buffer_end);
            buffer_end += 4;

            // tags
            System.arraycopy(hashed.tagsBuffer(), hashed.tagBufferStart(), buffer, buffer_end, hashed.tagBufferLength());
            buffer_end += hashed.tagBufferLength();

            writeMetric(hashed);

            last_tags_hash = hashed.tagsSetHash();
        }
    }

    void writeMetric(final LowLevelMetricData.HashedLowLevelMetricData hashed) {
        // series hash
        Bytes.setLong(buffer, hashed.timeSeriesHash(), buffer_end);
        buffer_end += 8;

        Bytes.setInt(buffer, hashed.metricLength(), buffer_end);
        buffer_end += 4;

        System.arraycopy(hashed.metricBuffer(), hashed.metricStart(), buffer, buffer_end, hashed.metricLength());
        buffer_end += hashed.metricLength();

        switch (hashed.valueFormat()) {
            case INTEGER:
                Bytes.setLong(buffer, Double.doubleToRawLongBits((double) hashed.longValue()), buffer_end);
                break;
            case FLOAT:
                Bytes.setLong(buffer, Double.doubleToRawLongBits((double) hashed.floatValue()), buffer_end);
                break;
            case DOUBLE:
                Bytes.setLong(buffer, Double.doubleToRawLongBits(hashed.doubleValue()), buffer_end);
                break;
        }
        buffer_end += 8;
    }

    public void finishWrite() {
        Bytes.setInt(buffer, buffer_end - current_event_length_idx - 4, current_event_length_idx);
    }

    void require(int size) {
        if (buffer_end + size >= buffer.length) {
            byte[] temp = new byte[buffer.length * 2];
            System.arraycopy(buffer, 0, temp, 0, buffer_end);
            buffer = temp;
        }
    }

    public void setBuffer(final byte[] buffer) {
        this.buffer = buffer;
        buffer_offset = 0;
        buffer_end = buffer.length;
        tag_idx = 0;
        cur_read = 0;
        cur_idx = 0;
    }

    public void setBuffer(final byte[] buffer, final int offset, final int length) {
        this.buffer = buffer;
        buffer_offset = offset;
        buffer_end = buffer_offset + length;
        tag_idx = 0;
        cur_read = 0;
        cur_idx = 0;
    }

    @Override
    public boolean advance() {
        if (cur_idx >= buffer_end) {
            return false;
        }

        if (cur_read == buffer_offset || cur_read >= cur_length) {
            cur_read = 0;
            tag_idx = 0;
            int start = cur_idx;
            cur_length = Bytes.getInt(buffer, cur_idx);
            cur_idx += 4;

            timestamp = Bytes.getLong(buffer, cur_idx);
            cur_idx += 8;

            tags_hash = Bytes.getLong(buffer, cur_idx);
            cur_idx += 8;

            tags_length = Bytes.getInt(buffer, cur_idx);
            cur_idx += 4;

            tags_start = cur_idx;

            int last_t = tags_start;
            int i = tags_start;
            for (; i < tags_start + tags_length; i++) {
                if (buffer[i] == 0 || i + 1 == tags_start + tags_length) {
                    if (i + 1 == tags_start + tags_length) {
                        i++;
                    }
                    while (tag_idx + 2 >= tag_indices.length) {
                        int[] temp = new int[tag_indices.length * 2];
                        System.arraycopy(tag_indices, 0, temp, 0, tag_indices.length);
                        tag_indices = temp;
                    }
                    tag_indices[tag_idx++] = last_t;
                    tag_indices[tag_idx++] = i - last_t;
                    last_t = i + 1;
                }
            }
            tags_count = tag_idx / 4;
            cur_idx += tags_length;

            series_hash = Bytes.getLong(buffer, cur_idx);
            cur_idx += 8;

            metric_length = Bytes.getInt(buffer, cur_idx);
            cur_idx += 4;
            metric_start = cur_idx;

            cur_idx += metric_length;
            metric_hash = XXHash.hash(buffer, metric_start, metric_length);

            value = Double.longBitsToDouble(Bytes.getLong(buffer, cur_idx));
            cur_idx += 8;
            cur_read += cur_idx - start;
            cur_tag_idx = -4;
            return true;
        } else {
            // reset tag idx
            cur_tag_idx = -4;

            series_hash = Bytes.getLong(buffer, cur_idx);
            cur_idx += 8;

            // just the metric bit
            metric_length = Bytes.getInt(buffer, cur_idx);
            cur_idx += 4;

            metric_start = cur_idx;
            cur_idx += metric_length;
            metric_hash = XXHash.hash(buffer, metric_start, metric_length);

            value = Double.longBitsToDouble(Bytes.getLong(buffer, cur_idx));
            cur_idx += 8;
            cur_read += 8 + 4 + 8 + metric_length;
            return true;
        }
    }

    @Override
    public boolean hasParsingError() {
        return false;
    }

    @Override
    public String parsingError() {
        return null;
    }

    @Override
    public StringFormat metricFormat() {
        return StringFormat.UTF8_STRING;
    }

    @Override
    public int metricStart() {
        return metric_start;
    }

    @Override
    public int metricLength() {
        return metric_length;
    }

    @Override
    public byte[] metricBuffer() {
        return buffer;
    }

    @Override
    public ValueFormat valueFormat() {
        return ValueFormat.DOUBLE;
    }

    @Override
    public long longValue() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public float floatValue() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public TimeStamp timestamp() {
        // TODO - wasteful
        return new SecondTimeStamp(timestamp);
    }

    @Override
    public byte[] tagsBuffer() {
        return buffer;
    }

    @Override
    public int tagBufferStart() {
        return tags_start;
    }

    @Override
    public int tagBufferLength() {
        return tags_length;
    }

    @Override
    public StringFormat tagsFormat() {
        return StringFormat.UTF8_STRING;
    }

    @Override
    public byte tagDelimiter() {
        return 0;
    }

    @Override
    public int tagSetCount() {
        return tags_count;
    }

    @Override
    public boolean advanceTagPair() {
        cur_tag_idx +=4;
        if (cur_tag_idx >= tag_idx) {
            return false;
        }
        return true;
    }

    @Override
    public int tagKeyStart() {
        if (cur_tag_idx == -4) {
            return tag_indices[0];
        }
        return tag_indices[cur_tag_idx];
    }

    @Override
    public int tagKeyLength() {
        if (cur_tag_idx == -4) {
            return tag_indices[1];
        }
        return tag_indices[cur_tag_idx + 1];
    }

    @Override
    public int tagValueStart() {
        if (cur_tag_idx == -4) {
            return tag_indices[2];
        }
        return tag_indices[cur_tag_idx + 2];
    }

    @Override
    public int tagValueLength() {
        if (cur_tag_idx == -4) {
            return tag_indices[3];
        }
        return tag_indices[cur_tag_idx + 3];
    }

    @Override
    public void close() {
        last_tags_hash = -1;
        buffer_end = 0;
        current_event_length_idx = 0;

        buffer_offset = 0;
        cur_length = 0;
        cur_idx = 0;
        cur_read = 0;
        tags_start = 0;
        tags_length = 0;
        tags_count = 0;
        metric_start = 0;
        metric_length = 0;
        tag_idx = 0;
        cur_tag_idx = 0;
        release();
    }

    @Override
    public long timeSeriesHash() {
        return series_hash;
    }

    @Override
    public long tagsSetHash() {
        return tags_hash;
    }

    @Override
    public long tagPairHash() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long tagKeyHash() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long tagValueHash() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long metricHash() {
        return metric_hash;
    }

    @Override
    public boolean commonTags() {
        // TODO - maybe, maybe not
        return false;
    }

    @Override
    public boolean commonTimestamp() {
        // TODO - maybe, maybe not
        return false;
    }

    @Override
    public void run() {
        try {
            shard.addEvent(this);
        } catch (Throwable t) {
            logger.info("Unexpected exception processing low level data", t);
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder()
                .append("bufEnd=")
                .append(buffer_end)
                .append(", bufferOffset=")
                .append(buffer_offset)
                .append(", curLength=")
                .append(cur_length)
                .append(", curIdx=")
                .append(cur_idx)
                .append(", curRead=")
                .append(cur_read)
                .append(", tagsStart=")
                .append(tags_start)
                .append(", tagsLength=")
                .append(tags_length)
                .append(", tagsCount=")
                .append(tags_count)
                .append(", metricStart=")
                .append(metric_start)
                .append(", metricLength=")
                .append(metric_length)
                .append(", tagIdx=")
                .append(tag_idx)
                .append(", curTagIdx=")
                .append(cur_tag_idx)
                .append(", timestamp=")
                .append(timestamp)
                .append(", value=")
                .append(value)
                .append(", buffer=[")
                .append(new String(buffer, 0, buffer_end))
                .append("]");

        return buf.toString();
    }

    @Override
    public Object object() {
        return this;
    }

    @Override
    public void release() {
        if (this.pooled_object != null) {
            this.pooled_object.release();
        }
    }

    @Override
    public void setPooledObject(PooledObject pooled_object) {
        if (this.pooled_object != null) {
            this.pooled_object.release();
        }
        this.pooled_object = pooled_object;
    }
}
