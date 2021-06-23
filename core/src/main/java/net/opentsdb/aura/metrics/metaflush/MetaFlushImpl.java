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

package net.opentsdb.aura.metrics.metaflush;

import net.opentsdb.aura.metrics.core.FlushStatus;
import net.opentsdb.aura.metrics.core.Flusher;
import net.opentsdb.aura.metrics.core.TimeSeriesRecord;
import net.opentsdb.aura.metrics.core.TimeSeriesRecordFactory;
import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.data.HashTable;
import net.opentsdb.aura.metrics.core.data.Memory;
import io.ultrabrew.metrics.Counter;
import io.ultrabrew.metrics.MetricRegistry;
import io.ultrabrew.metrics.Timer;
import net.opentsdb.collections.LongLongHashTable;
import net.opentsdb.collections.LongLongIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MetaFlushImpl implements Flusher {

    private final UploaderFactory uploaderFactory;
    private final ExecutorService pool ;
    private final TimeSeriesRecordFactory timeSeriesRecordFactory;
    private final MetaWriterFactory metaWriterFactory;
    private final MetricRegistry registry;
    private final String namespace;
    private final int frequency;

    public MetaFlushImpl(
            TimeSeriesRecordFactory timeSeriesRecordFactory,
            MetaWriterFactory metaWriterFactory,
            UploaderFactory uploaderFactory,
            ExecutorService service,
            MetricRegistry registry,
            String namespace,
            int flushFrequency) {

        this.timeSeriesRecordFactory = timeSeriesRecordFactory;
        this.metaWriterFactory = metaWriterFactory;
        this.uploaderFactory = uploaderFactory;
        this.pool = service;
        this.registry = registry;
        this.namespace = namespace;
        this.frequency = flushFrequency;
    }

    @Override
    public long frequency() {
        return frequency;
    }

    @Override
    public FlushStatus flushShard(final int shardId,
                                  final HashTable tagTable,
                                  final HashTable metricTable,
                                  final LongLongHashTable timeSeriesTable,
                                  int flushTimestamp) {
        final Future submit = pool.submit(new FlushJob(registry, shardId, tagTable, metricTable, timeSeriesTable, flushTimestamp));
        return new MetaFlushStatus(submit);
    }

    private class MetaFlushStatus implements FlushStatus {

        private final Future future;

        public MetaFlushStatus(Future future) {

            this.future = future;
        }

        @Override
        public boolean inProgress() {
            return !future.isDone();
        }

    }


    public class FlushJob implements Runnable {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final MetricRegistry registry;
        private final int shardId;
        private final HashTable tagTable;
        private final HashTable metricTable;
        private final LongLongHashTable timeSeriesTable;
        private final int flushTimestamp;
        private final TimeSeriesRecord record;
        private final byte[] pointerBuffer;
        private byte[] byteBuffer;
        private final Counter timeseriesWritten;
        private final Timer flushDuration;
        private final Counter flushErrors;
        private final String[] tags;
        public FlushJob(final MetricRegistry registry,
                        final int shardId,
                        final HashTable tagTable,
                        final HashTable metricTable,
                        final LongLongHashTable timeSeriesTable,
                        int flushTimestamp) {
            this.registry = registry;

            this.shardId = shardId;
            this.tagTable = tagTable;
            this.metricTable = metricTable;
            this.timeSeriesTable = timeSeriesTable;
            this.flushTimestamp = flushTimestamp;
            this.record = timeSeriesRecordFactory.create();
            this.pointerBuffer = new byte[HashTable.valSz + 4];
            this.byteBuffer = new byte[2048];
            this.timeseriesWritten = registry.counter("timeseries.flushed");
            this.flushDuration = registry.timer("meta.flush.duration");
            this.tags = new String[]{"namespace", namespace, "shard_id", String.valueOf(shardId)};
            this.flushErrors = registry.counter("meta.flush.errors");
        }

        @Override
        public void run() {
            final long start = this.flushDuration.start();
            try {
                final MetaWriter metaWriter = metaWriterFactory.create(uploaderFactory.create(shardId),shardId);
                metaWriter.init(flushTimestamp);
                log.info("Starting new meta flush for shard: {} and time: {}", shardId, flushTimestamp);
                final LongLongIterator iterator = timeSeriesTable.iterator();
                long addr = 0, hash= 0;
                int count = 0;
                int countInvalidTag = 0;
                int countInvalidMetric = 0;
                while (iterator.hasNext()) {
                    iterator.next();
                    hash = iterator.key();
                    addr = iterator.value();
                    if (addr == 0) {
                        break;
                    }
                    metaWriter.advanceRecord();
                    metaWriter.writeHash(hash);
                    record.open(addr);

                    final long tagKey = record.getTagKey();
                    final long metricKey = record.getMetricKey();

                    if (tagTable.containsKey(tagKey)) {
                        tagTable.getPointer(tagKey, pointerBuffer);
                        long tagAddress = ByteArrays.getLong(pointerBuffer, 0);
                        int tagLength = ByteArrays.getInt(pointerBuffer, 8);
                        if (tagLength > byteBuffer.length) {
                            this.byteBuffer = new byte[tagLength];
                        }
                        Memory.read(tagAddress, byteBuffer, tagLength);
                        metaWriter.writeTag(byteBuffer, 0, tagLength);
                    } else {
                        countInvalidTag++;
                        //Should never come here
                    }
                    if (metricTable.containsKey(metricKey)) {
                        metricTable.getPointer(metricKey, pointerBuffer);
                        long metricAddress = ByteArrays.getLong(pointerBuffer, 0);
                        int metricLength = ByteArrays.getInt(pointerBuffer, 8);
                        if (metricLength > byteBuffer.length) {
                            this.byteBuffer = new byte[metricLength];
                        }
                        Memory.read(metricAddress, byteBuffer, metricLength);
                        metaWriter.writeMetric(byteBuffer, 0, metricLength);
                    }

                    count++;
                    this.timeseriesWritten.inc(tags);
                    if (count % 100000 == 0 && log.isDebugEnabled()) {
                        log.debug("Wrote {} many records in meta flush for shard: {} and time: {}", count, shardId, flushTimestamp);
                    }
                }
                //For the last record
                metaWriter.advanceRecord();
                log.info("Done with this round of flushing {} records for shard: {} and time: {}", count, shardId, flushTimestamp);
                metaWriter.close();
            } catch (Throwable t) {
                log.error("Throwable in meta flush: ", t);
                this.flushErrors.inc(tags);
            }
            this.flushDuration.stop(start, tags);
        }
    }
}
