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

package net.opentsdb.aura.metrics;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.WritePolicy;
import net.opentsdb.aura.metrics.core.LongTermStorage;
import net.opentsdb.aura.metrics.core.RawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.data.ByteArrays;
import net.opentsdb.aura.metrics.core.downsample.DownSampledTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.GorillaRawTimeSeriesEncoder;
import net.opentsdb.aura.metrics.core.gorilla.OnHeapGorillaRawSegment;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.XXHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class LTSAerospike implements LongTermStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(LTSAerospike.class);

    public static final int KEY_LENGTH = 12;

    public static final String M_WRITE_EX = "aerospike.timeseries.write.exceptions";
    public static final String M_WRITE_SUCCESS = "aerospike.timeseries.write.success";

    public static final String M_READ_EX = "aerospike.timeseries.read.exceptions";
    public static final String M_READ_SUCCESS = "aerospike.timeseries.read.success";
    public static final String M_READ_FAILED = "aerospike.timeseries.read.failed";
    public static final String M_READ_HITS = "aerospike.timeseries.read.hits";
    public static final String M_READ_MISSES = "aerospike.timeseries.read.misses";
    public static final String M_READ_ITERATORS = "aerospike.timeseries.read.iterators";
    public static final String M_READ_LATENCY = "aerospike.timeseries.read.latency";

    private final StatsCollector stats;
    private final ThreadLocal<byte[]> keys = ThreadLocal.withInitial(() -> new byte[KEY_LENGTH]);
    private final ThreadLocal<EncoderValue> encoderValues = ThreadLocal.withInitial(() -> new EncoderValue());
    private final ThreadLocal<DSEncodeMapValue> encodedMapValues = ThreadLocal.withInitial(() -> new DSEncodeMapValue());

    private final ASSyncClient asClient = null;
    private final ASBatchClient batchClient = null;

    private final AerospikeClient aerospikeClient;
    private final WritePolicy writePolicy;
    private final MapPolicy mapPolicy;

    private final int secondsInRecord;
    private final byte[] set;
    private final String binName;
    private final byte[] bin;
    private final int segmentsInRecord;
    private final int secondsInSegment;

    private volatile long writeTSExceptions;
    private volatile long writeTSSuccess;
    private volatile long readTSExceptions;
    private volatile long readTSSuccess;
    private volatile long readTSErrors;
    private volatile long readTSHits;
    private volatile long readTSMisses;
    private volatile long readTSiterators;

    private String namespace;
    private String setName;
    private String setNameHex;

    public LTSAerospike(final ASCluster cluster,
                 final String namespace,
                 final int secondsInRecord,
                 final int secondsInSegment,
                 final String setName,
                 final String binName,
                 final StatsCollector stats,
                 final ScheduledExecutorService executor) {
        this.setName = setName;
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("Unable to start Aerospike cluster client without a cluster namespace.");
        }

        this.stats = stats;
        this.namespace = namespace;
        this.secondsInRecord = secondsInRecord;
        this.secondsInSegment = secondsInSegment;
        segmentsInRecord = secondsInRecord / secondsInSegment;
//        asClient = new ASSyncClient(cluster.cluster(), namespace, metricRegistry, executor);
//        batchClient = new ASBatchClient(cluster.cluster(), namespace, metricRegistry, executor);

        // TODO: fill in the policy and host details
        aerospikeClient = new AerospikeClient(null, cluster.getHosts());
        writePolicy = new WritePolicy();
        mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);

        // we hash the namespace to get the set name.
        // TODO - rather have a way to hash to the max # of sets so we don't need
        // a full 8 bytes
        set = new byte[8];
        ByteArrays.putLong(XXHash.hash(setName), set,0);
        setNameHex = Buffer.bytesToHexString(set);
        this.binName = binName;
        LOGGER.info("Aerospike namespace: {} set: {} bin: {}", namespace, setNameHex, binName);
        bin = binName.getBytes(StandardCharsets.UTF_8);

        executor.scheduleAtFixedRate(new Metrics(),
                MINUTES.toMillis(1),
                MINUTES.toMillis(1),
                MILLISECONDS);

        LOGGER.info("Successfully started up AS client.");
    }

    /**
     * @return The hash of the Aerospike set. Used for UTs right now.
     */
    public byte[] getASSetHash() {
        return set;
    }

    @Override
    public boolean flush(long hash, TimeSeriesEncoder encoder) {
        try {
            int recordTimestamp = encoder.getSegmentTime() -
                    (encoder.getSegmentTime() % secondsInRecord);

            byte[] rowKey = keys.get();
            ByteArrays.putLong(hash, rowKey, 0);
            ByteArrays.putInt(recordTimestamp, rowKey, 8);
            Key key = new Key(namespace, setNameHex, rowKey);

            int offset = (encoder.getSegmentTime() - recordTimestamp) / secondsInSegment;

            if(encoder instanceof DownSampledTimeSeriesEncoder) {
                DSEncodeMapValue dsEncodeMapValue = encodedMapValues.get();
                dsEncodeMapValue.reset((DownSampledTimeSeriesEncoder) encoder, offset);
                aerospikeClient.operate(
                    writePolicy, key, MapOperation.putItems(mapPolicy, null, dsEncodeMapValue));
            } else {
                EncoderValue encoderValue = encoderValues.get();
                encoderValue.setEncoder(encoder);
                aerospikeClient.operate(writePolicy, key, MapOperation.put(mapPolicy, null, Value.get(offset), encoderValue));
            }
            writeTSSuccess++;
            return true;
        } catch (Throwable t) {
            LOGGER.error("WTF? Failed AS", t);
            writeTSExceptions++;
            return false;
        }
    }

    @Override
    public Records read(long hash, int startTimestamp, int endTimestamp) {
        RecordsImp r = new RecordsImp(hash, startTimestamp, endTimestamp);
        ++readTSiterators;
        // find the first record or if nothing was there, bummer
        r.advance();
        return r;
    }

  /**
   * Issues a batch request to Aerospike given the keys and length.
   *
   * @param keys The keys array.
   * @param keyLength The length of the keys to read (When re-using the buffer).
   * @return The batch record iterator.
   */
  public Record[] batchRead(final byte[][] keys, final int keyLength) {
    Key[] keyList = new Key[keyLength];
    for (int i = 0; i < keyLength; i++) {
      byte[] rowKey = keys[i];
      Key key = new Key(namespace, setNameHex, new ByteValue(rowKey));
      keyList[i] = key;
    }
    return aerospikeClient.get(null, keyList, binName);
  }

  /**
   * Issues a batch request to Aerospike given the keys and length.
   *
   * @param keys The keys array.
   * @param keyLength The length of the keys to read (When re-using the buffer).
   * @return The batch record iterator.
   */
  public Record[] batchRead(
      final byte[][] keys, final int keyLength, final String binName, final int aggOrdinal) {

    List<Value> entryList = new ArrayList<>();
    for (int recordOffset = 0; recordOffset < 3; recordOffset++) {
        entryList.add(Value.get((byte) (recordOffset << 4 | 0))); // header
        entryList.add(Value.get((byte) (recordOffset << 4 | aggOrdinal)));
    }

    Key[] rowKeys = new Key[keyLength];
    for (int i = 0; i < keyLength; i++) {
      byte[] rowKey = keys[i];
      rowKeys[i] = new Key(namespace, setNameHex, new ByteValue(rowKey));
    }

    Operation operation = MapOperation.getByKeyList(binName, entryList, MapReturnType.KEY_VALUE);
    return aerospikeClient.get(null, rowKeys, operation);
  }

    /**
     * @return The number of seconds in a record.
     */
    public int secondsInRecord() {
        return secondsInRecord;
    }

    /**
     * This handles querying aerospike and iterating over the results WITHOUT having
     * much in memory at one time. The records in AS will only have X number of
     * segments but queries may cover multiple records. Therefore we'll read ONE
     * record from AS at a time (and possibly just a segment or two out of that record)
     * and let the query layer process it into the proper downsample slot or group by
     * array, etc. Then the query layer asks for the next record when ready.
     *
     * The way it works is this:
     * <ol>
     *   <li>The read() method above calls {@link #advance()} to find the FIRST record
     *   from AS in the time range given. Check the TimeSeriesEncoder segment
     *   time to find the actual base timestamp as it could be far from the start
     *   time. Or there may be nothing. The iterator is ready either way.</li>
     *   <li>The querant calls {@link #hasNext()} to see if there is more data to
     *   read. If not, bail, no more data.</li>
     *   <li>But if true, then call {@link #next()} to get the
     *   next segment. Remember that the segment could be OUT OF ORDER or
     *   there could be gaps from the previous segment.</li>
     * </ol>
     *
     */
    public class RecordsImp implements Records {
        private long hash;
        private int startTimestamp;
        private int endTimestamp;
        private List<Map.Entry<Long, byte[]>> mapEntries;
        private int segmentTimestamp;
        private int recordTimestamp;

        RecordsImp(long hash, int startTimestamp, int endTimestamp) {
            this.hash = hash;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
            segmentTimestamp = startTimestamp;
            recordTimestamp = segmentTimestamp - (segmentTimestamp % secondsInRecord);
        }

        void advance() {
            while (true) {
                if (readNext()) {
                    break;
                }
                if (recordTimestamp >= endTimestamp) {
                    segmentTimestamp = endTimestamp;
                    break;
                }
                segmentTimestamp = recordTimestamp;
            }
        }

        @Override
        public boolean hasNext() {
            if (segmentTimestamp >= endTimestamp) {
                return false;
            }
            return true;
        }

        @Override
        public RawTimeSeriesEncoder next() {
            for (int i = 0; i < segmentsInRecord; i++) {
                // UGG!! Because we may be missing segments and the AS client returns
                // maps as entries in a list... we can't just go to an index. We have
                // to iterate and check the header.
                int offset = computeOffset(segmentTimestamp);

                Iterator<Map.Entry<Long, byte[]>> iterator = mapEntries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, byte[]> entry = iterator.next();
                    if (entry != null && entry.getKey() == offset) {
                        byte[] data = entry.getValue();
                        iterator.remove();

                        // TODO - reuse!!!!
                        OnHeapGorillaRawSegment segment = new OnHeapGorillaRawSegment(segmentTimestamp, data, 0, data.length);
                        segmentTimestamp += secondsInSegment; // important to advance.

                        if (mapEntries.isEmpty()) {
                            // find the next record if we have one
                            mapEntries = null;
                            advance();
                        }

                        // TODO - reuse!!!!
                        return new GorillaRawTimeSeriesEncoder(false, null, segment, null);
                    }
                }
                segmentTimestamp += secondsInSegment;
            }
            ++readTSExceptions;
            throw new IllegalStateException(
                    "Shouldn't be here! Start time " + startTimestamp
                          + " end time " + endTimestamp
                          + " segment time " + segmentTimestamp
                          +" record time " + recordTimestamp
                          +" Map " + mapEntries);
        }

        int computeOffset(int segmentTimestamp) {
            return segmentsInRecord - ((recordTimestamp - segmentTimestamp) / secondsInSegment);
        }

        boolean readNext() {
            mapEntries = null;
            if (recordTimestamp >= endTimestamp) {
                LOGGER.info("LTS: read next past end time.");
                return false;
            }

            byte[] key = keys.get();
            ByteArrays.putLong(hash, key, 0);
            ByteArrays.putInt(recordTimestamp, key, 8);

            recordTimestamp += secondsInRecord;

            // don't ask for more than we need.
            int offset = segmentsInRecord - ((recordTimestamp - segmentTimestamp) / secondsInSegment);
            int end = Math.min(segmentsInRecord, (endTimestamp - (recordTimestamp - secondsInRecord)) / secondsInSegment);

            // TODO - watch this as it's expensive.
            final long start = DateTime.nanoTime();
//            RecordIterator iterator = asClient.mapRangeQuery(key, set, bin, offset, end);
            RecordIterator iterator = null;

            stats.addTime(M_READ_LATENCY, DateTime.nanoTime() - start, ChronoUnit.NANOS);
            if (iterator.getResultCode() != ResultCode.OK) {
                if (iterator.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                    ++readTSMisses;
                } else {
                    ++readTSErrors;
                    LOGGER.warn("No data for AS call on : " + Arrays.toString(key) + " @" + recordTimestamp + " => "
                            + iterator.getResultCode() + " == " + (ResultCode.getResultString(iterator.getResultCode())));
                    // TODO - bail on some errors in which we can't retry or if the server
                    // is busy we should backoff a bit.
                }
                return false;
            }

            if (!iterator.advance()) {
                // TODO - hmm?
                ++readTSExceptions;
                LOGGER.warn("WTF Response was OK but no data in iterator @" + (recordTimestamp - secondsInRecord));
                return false;
            }

            // TODO - EWW!!! Wasteful! Figure out how to work with the underlying buffer.
            mapEntries = (List<Map.Entry<Long, byte[]>>) iterator.valueToObject();
            if (mapEntries.isEmpty()) {
                ++readTSExceptions;
                LOGGER.warn("WTF Map was empty @" + (recordTimestamp - secondsInRecord));
                mapEntries = null;
                return false;
            }
            ++readTSHits;
            return true;
        }
    }

    class Metrics implements Runnable {
        private long prevWriteTSExceptions;
        private long prevWriteTSSuccess;
        private long prevReadTSExceptions;
        private long prevReadTSSuccess;
        private long prevReadTSErrors;
        private long prevReadTSHits;
        private long prevReadTSMisses;
        private long prevReadTSiterators;

        @Override
        public void run() {
            // WRITE metrics
            long temp = writeTSExceptions;
            long delta = temp - prevWriteTSExceptions;
            prevWriteTSExceptions = temp;
            stats.incrementCounter(M_WRITE_EX, delta);

            temp = writeTSSuccess;
            delta = temp - prevWriteTSSuccess;
            prevWriteTSSuccess = temp;
            stats.incrementCounter(M_WRITE_SUCCESS, delta);

            // READ metrics
            temp = readTSErrors;
            delta = temp - prevReadTSErrors;
            prevReadTSErrors = temp;
            stats.incrementCounter(M_READ_FAILED, delta);

            temp = readTSExceptions;
            delta = temp - prevReadTSExceptions;
            prevReadTSExceptions = temp;
            stats.incrementCounter(M_READ_EX, delta);

            temp = readTSHits;
            delta = temp - prevReadTSHits;
            prevReadTSHits = temp;
            stats.incrementCounter(M_READ_HITS, delta);

            temp = readTSMisses;
            delta = temp - prevReadTSMisses;
            prevReadTSMisses = temp;
            stats.incrementCounter(M_READ_MISSES, delta);

            temp = readTSSuccess;
            delta = temp - prevReadTSSuccess;
            prevReadTSSuccess = temp;
            stats.incrementCounter(M_READ_SUCCESS, delta);

            temp = readTSiterators;
            delta = temp - prevReadTSiterators;
            prevReadTSiterators = temp;
            stats.incrementCounter(M_READ_ITERATORS, delta);
        }
    }
}