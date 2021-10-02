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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.command.SyncCommand;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;
import net.opentsdb.aura.metrics.operation.MapOperation;
import net.opentsdb.aura.metrics.operation.MapPolicy;
import net.opentsdb.aura.metrics.operation.OperationLocal;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A pared down Aerospike client that attempts to avoid copying byte arrays and
 * instantiating objects as much as possible. The client is thread safe and makes use of
 * thread locals. Instantiate one of these per namespace per app and share. Return
 * codes are used instead of {@link AerospikeException}s as well
 * though some exceptions may be thrown.
 * <p>
 * <b>WARNING:</b> Once you call a write or read method, do NOT pass the results
 * to another thread or modify any of the given arrays in another thread otherwise
 * the results will be indeterminate.
 *
 * TODO - We can make the reads a bit more efficient by unpacking the unpacking library
 * and pulling out it's methods into the record iterator.
 * TODO - There will likely be more calls to implement.
 * TODO - May want to make the namespace a param for calls as well.
 * TODO - We're only dealing with single calls for now. Batching can come later and
 * async as well. Throttling is a pain in the neck with async as is safely handling
 * all of the arrays.
 * TODO - Some logging please.
 */
public class ASSyncClient {
//    private static final Logger LOGGER = LoggerFactory.getLogger(ASSyncClient.class);
//
//    public static final String M_TCP = "AerospikeClient.tcp.call.latency";
//    public static final String M_OVERALL = "AerospikeClient.overall.call.latency";
//
//    public static final String M_RAW_WRITES = "AerospikeClient.raw.writes.attempts";
//    public static final String M_RAW_READS = "AerospikeClient.raw.reads.attempts";
//    public static final String M_MAP_WRITES = "AerospikeClient.map.writes.attempts";
//    public static final String M_MAP_READS = "AerospikeClient.map.reads.attempts";
//
//    public static final String M_IO_TIMEOUTS = "AerospikeClient.io.timeouts";
//    public static final String M_IO_EX = "AerospikeClient.io.exceptions";
//    public static final String M_IO_RETRIES = "AerospikeClient.io.retries";
//    public static final String M_IO_IOEX = "AerospikeClient.io.ioExceptions";
//    public static final String M_IO_SOT = "AerospikeClient.io.socketTimeout";
//    public static final String M_IO_CLOSED = "AerospikeClient.io.closedNodeConnections";
//    public static final String M_IO_REPOOLED = "AerospikeClient.io.rePooledConnections";
//    public static final String M_IO_BYTES_SENT = "AerospikeClient.io.bytesSent";
//    public static final String M_IO_BYTES_READ = "AerospikeClient.io.bytesRead";
//    public static final String M_IO_BYTES_SKIPPED = "AerospikeClient.io.bytesSkipped";
//
//    /** The cluster we're working with. */
//    private final Cluster cluster;
//
//    /** The namespace this client works on. */
//    private final String namespace;
//
//    /** Where we're reporting metrics. */
//    private final StatsCollector stats;
//
//    /** The thread local commands list. */
//    private ThreadLocal<ASCommand> operations;
//
//    /** Policies. TODO - set these. */
//    private final WritePolicy defaultWritePolicy;
//    private final Policy defaultReadPolicy;
//
//    /** Metric tags. */
//    private final String[] tags;
//
//    // Interactions
//    private volatile long rawWrites;
//    private volatile long rawReads;
//    private volatile long mapWrites;
//    private volatile long mapReads;
//
//    // AS IO
//    private volatile long timeouts;
//    private volatile long exceptions;
//    private volatile long retries;
//    private volatile long ioExceptions;
//    private volatile long socketTimeouts;
//    private volatile long closedNodeConnections;
//    private volatile long rePooledConnections;
//    private volatile long bytesSent;
//    private volatile long bytesRead;
//    private volatile long bytesDiscarded;
//
//    /**
//     * Default ctor.
//     * @param cluster The non-null cluster to work with.
//     * @param namespace The non-null namespace to write to.
//     * @param stats The metric registry to write metrics to.
//     * @param executorService A thread to schedule writes to the registry for
//     *                        high-perf counters, etc.
//     */
//    public ASSyncClient(final Cluster cluster,
//                        final String namespace,
//                        final StatsCollector stats,
//                        final ScheduledExecutorService executorService) {
//        this.cluster = cluster;
//        this.namespace = namespace;
//        this.stats = stats;
//        operations = new ThreadLocal<ASCommand>() {
//            @Override
//            protected ASCommand initialValue() {
//                return new ASCommand();
//            }
//        };
//        defaultWritePolicy = new WritePolicy();
//        defaultReadPolicy = new Policy();
//        tags = new String[] { "namespace", namespace, "callType", "singleSync" };
//        executorService.scheduleAtFixedRate(new MetricFlusher(),
//                TimeUnit.MINUTES.toMillis(1),
//                TimeUnit.MINUTES.toMillis(1),
//                TimeUnit.MILLISECONDS);
//    }
//
//    /**
//     * Attempts to write or overwrite the record in the cluster.
//     * @param key The non-null and non-empty key to write.
//     * @param setBytes An optional set.
//     * @param bin An optional bin.
//     * @param value The non-null (I think) value to write.
//     * @return A {@link ResultCode}.
//     */
//    public int write(final byte[] key,
//                     final byte[] setBytes,
//                     final byte[] bin,
//                     final byte[] value) {
//        ++rawWrites;
//        final ASCommand cmd = operations.get();
//        cmd.byteValue.bytes = value;
//        final OperationLocal op = new OperationLocal(
//                Operation.Type.WRITE,
//                bin,
//                cmd.byteValue);
//        return cmd.write(defaultWritePolicy, key, setBytes, op);
//    }
//
//    /**
//     * Attempts to write or overwrite the key in the map in the record. These keys are
//     * 4 byte integers.
//     * TODO - may want to do shorts or even bytes.
//     * @param key The non-null and non-empty key to write.
//     * @param setBytes An optional set.
//     * @param bin An optional bin.
//     * @param mapKey The key for the map entry.
//     * @param value The non-null (I think) value to write.
//     * @return A {@link ResultCode}.
//     */
//    public int putInMap(final byte[] key,
//                        final byte[] setBytes,
//                        final byte[] bin,
//                        final int mapKey,
//                        final byte[] value) {
//        ++mapWrites;
//        final ASCommand cmd = operations.get();
//        cmd.byteValue.bytes = value;
//        final OperationLocal op = cmd.mapOp.put(
//                MapPolicy.Default,
//                bin,
//                mapKey,
//                cmd.byteValue);
//        return cmd.write(defaultWritePolicy, key, setBytes, op);
//    }
//
//    /**
//     * Attempts to write or overwrite the key in the map in the record. These keys are
//     * 4 byte integers.
//     * TODO - may want to do shorts or even bytes.
//     * @param key The non-null and non-empty key to write.
//     * @param setBytes An optional set.
//     * @param bin An optional bin.
//     * @param mapKey The key for the map entry.
//     * @param value The non-null (I think) value to write.
//     * @return A {@link ResultCode}.
//     */
//    public int putInMap(final byte[] key,
//                        final byte[] setBytes,
//                        final byte[] bin,
//                        final int mapKey,
//                        final Value value) {
//        ++mapWrites;
//        final ASCommand cmd = operations.get();
//        final OperationLocal op = cmd.mapOp.put(MapPolicy.Default, bin, mapKey, value);
//        return cmd.write(defaultWritePolicy, key, setBytes, op);
//    }
//
//    /**
//     * Attempts to read the record from the cluster. See
//     * {@link RecordIterator#getResultCode()} before advancing the records.
//     * @param key The non-null and non-empty key to fetch.
//     * @param setBytes An optional set.
//     * @param bin An optional bin.
//     * @return The thread-local {@link RecordIterator}, always non-null.
//     */
//    public RecordIterator get(final byte[] key,
//                              final byte[] setBytes,
//                              final byte[] bin) {
//        ++rawReads;
//        final ASCommand cmd = operations.get();
//        final OperationLocal op = new OperationLocal(Operation.Type.READ, bin);
//        return cmd.read(defaultReadPolicy, key, setBytes, op);
//    }
//
//    /**
//     * Attempts to read a range of map entries from the cluster. See
//     * {@link RecordIterator#getResultCode()} before advancing the records.
//     * @param key The non-null and non-empty key to fetch.
//     * @param setBytes An optional set.
//     * @param bin An optional bin.
//     * @param start The first key to read, inclusive.
//     * @param end The key to read up to, exclusive.
//     * @return The thread-local {@link RecordIterator}, always non-null.
//     */
//    public RecordIterator mapRangeQuery(final byte[] key,
//                                        final byte[] setBytes,
//                                        final byte[] bin,
//                                        final int start,
//                                        final int end) {
//        ++mapReads;
//        final ASCommand cmd = operations.get();
//        final OperationLocal op = cmd.mapOp.getByKeyRange(bin, start, end, MapReturnType.KEY_VALUE);
//        return cmd.read(defaultReadPolicy, key, setBytes, op);
//    }
//
//    /**
//     * <b>WARNING:</b> This will shutdown the CLUSTER passed here! Only call it if you are
//     * finished with all clients. Or just close the cluster directly. There isn't anything
//     * to shutdown here EXCEPT for the metric gathering task that's in the shared scheduler
//     * thread. This client will stick around until that executor is killed.
//     */
//    public void shutdown() {
//        cluster.close();
//    }
//
//    /**
//     * A thread local command that is reset on each of the preceding calls with
//     * memory re-used as much as possible.
//     */
//    private class ASCommand extends SyncCommand {
//        /** The policy we're currently working with. */
//        private Policy policy;
//
//        /** Hasher that avoids creating new byte arrays. */
//        private UpdatingRipeMD160 hasher;
//
//        /** The AS Set we're writing to. Optional. */
//        private byte[] setBytes;
//
//        /** The operating args. */
//        private OperateArgs args;
//
//        /** The operation we're executing. */
//        private OperationLocal op;
//
//        /** An optional map operation. */
//        private MapOperation mapOp;
//
//        /** The results of a call if it returns data. */
//        private RecordIterator recordIterator;
//
//        /** A value to store the byte array when applicable. */
//        private ByteValue byteValue;
//
//        /**
//         * Defautl ctor.
//         */
//        ASCommand() {
//            super();
//            hasher = new UpdatingRipeMD160();
//            args = new OperateArgs();
//            mapOp = new MapOperation();
//            recordIterator = new RecordIterator();
//            byteValue = new ByteValue(null);
//        }
//
//        /**
//         * Resets the state.
//         */
//        private void clearOperationArgs() {
//            args.hasWrite = false;
//            args.readAttr = 0;
//            args.size = 0;
//            args.writeAttr = 0;
//        }
//
//        /**
//         * Hopefully obvious, attempts to write data to the cluster.
//         * @param writePolicy The non-null write policy.
//         * @param key The non-null and non-empty key.
//         * @param setBytes An optional set.
//         * @param op The non-null operation to execute.
//         * @return A {@link ResultCode}.
//         */
//        private int write(final WritePolicy writePolicy,
//                          final byte[] key,
//                          final byte[] setBytes,
//                          final OperationLocal op) {
//            clearOperationArgs();
//            args.writeAttr = Command.INFO2_WRITE;
//            args.hasWrite = true;
//
//            this.policy = writePolicy;
//            this.setBytes = setBytes;
//            hasher.update(setBytes, 0, setBytes.length);
//            hasher.update((byte) ParticleType.BLOB);
//            hasher.update(key, 0, key.length);
//            hasher.digest(); // updates in the ByteValue
//            this.op = op;
//
//            // estimate the size and set our dataOffset:
//            dataOffset += op.binName.length + OPERATION_HEADER_SIZE;
//            dataOffset += op.value.estimateSize();
//
//            if (writePolicy.respondAllOps) {
//                args.writeAttr |= Command.INFO2_RESPOND_ALL_OPS;
//            }
//            args.size = dataOffset;
//
//            return executeLocal(cluster, writePolicy, null, false);
//        }
//
//        /**
//         * Hopefully obvious. Attempts to read something from the cluster.
//         * @param policy The non-null write policy.
//         * @param key The non-null and non-empty key.
//         * @param setBytes An optional set.
//         * @param op The non-null operation to execute.
//         * @return A {@link ResultCode}.
//         */
//        private RecordIterator read(final Policy policy,
//                                    final byte[] key,
//                                    final byte[] setBytes,
//                                    final OperationLocal op) {
//            clearOperationArgs();
//            this.policy = policy;
//            hasher.update(setBytes, 0, setBytes.length);
//            hasher.update((byte) ParticleType.BLOB);
//            hasher.update(key, 0, key.length);
//            hasher.digest(); // updates in the ByteValue
//            this.op = op;
//
//            // estimate the size and set our dataOffset:
//            dataOffset += op.binName.length + OPERATION_HEADER_SIZE;
//            dataOffset += op.value.estimateSize();
//
//            args.readAttr = Command.INFO1_READ;
//            args.size = dataOffset;
//
//            executeLocal(cluster, policy, null, false);
//            return recordIterator;
//        }
//
//        @Override
//        protected void writeBuffer() {
//            begin();
//            int fieldCount = estimateKeySize();
//            dataOffset += args.size;
//            sizeBuffer();
//
//            writeHeader(policy, args.readAttr, args.writeAttr, fieldCount, 1);
//            writeKey();
//
//            writeOperation(op);
//            end();
//        }
//
//        /**
//         * Local override to figure out how big the key is going to be. Increments
//         * the data offset.
//         * @return The number of fields.
//         */
//        private final int estimateKeySize() {
//            int fieldCount = 0;
//
//            if (namespace != null) {
//                dataOffset += Buffer.estimateSizeUtf8(namespace) + FIELD_HEADER_SIZE;
//                fieldCount++;
//            }
//
//            if (setBytes != null) {
//                dataOffset += setBytes.length + FIELD_HEADER_SIZE;
//                fieldCount++;
//            }
//
//            dataOffset += hasher.getDigest().length + FIELD_HEADER_SIZE;
//            fieldCount++;
//
//            /* NOT using at this time.
//            if (policy.sendKey) {
//                dataOffset += key.userKey.estimateSize() + FIELD_HEADER_SIZE + 1;
//                fieldCount++;
//            }*/
//            return fieldCount;
//        }
//
//        /**
//         * Writes what {@link #estimateKeySize()} estimated.
//         */
//        private final void writeKey() {
//            // Write key into buffer.
//            if (namespace != null) {
//                writeField(namespace, FieldType.NAMESPACE);
//            }
//
//            if (setBytes != null) {
//                writeField(setBytes, FieldType.TABLE);
//            }
//
//            writeField(hasher.getDigest(), FieldType.DIGEST_RIPE);
//
//            /* NOT using at this time
//            if (policy.sendKey) {
//                writeField(key.userKey, FieldType.KEY);
//            }*/
//        }
//
//        /**
//         * Serializes the operation.
//         * @param operation The non-null operation to serialize.
//         */
//        private void writeOperation(final OperationLocal operation) {
//            System.arraycopy(op.binName, 0, dataBuffer, dataOffset + OPERATION_HEADER_SIZE, op.binName.length);
//            int nameLength = op.binName.length;
//            int valueLength = operation.value.write(dataBuffer, dataOffset + OPERATION_HEADER_SIZE + nameLength);
//
//            Buffer.intToBytes(nameLength + valueLength + 4, dataBuffer, dataOffset);
//            dataOffset += 4;
//            dataBuffer[dataOffset++] = (byte) operation.type.protocolType;
//            dataBuffer[dataOffset++] = (byte) operation.value.getType();
//            dataBuffer[dataOffset++] = (byte) 0;
//            dataBuffer[dataOffset++] = (byte) nameLength;
//            dataOffset += nameLength + valueLength;
//        }
//
//        /**
//         * Cribbed from the super class. All of this to avoid allocations in the
//         * {@link Partition} class (which we still have to instantiate since the fields
//         * are final but at least it's small).
//         * @param cluster The non-null cluster.
//         * @param policy The non-null policy.
//         * @param node Optional node to write to.
//         * @param isRead Whether or not the op is a read or write.
//         * @return The {@link ResultCode}.
//         */
//        public int executeLocal(final Cluster cluster,
//                                final Policy policy,
//                                Node node,
//                                final boolean isRead) {
//            int partitionId = (Buffer.littleBytesToInt(hasher.getDigest(), 0) & 0xFFFF) % Node.PARTITIONS;
//            final Partition partition = new Partition(namespace, partitionId);
//            AerospikeException exception = null;
//            long deadline = 0;
//            int socketTimeout = policy.socketTimeout;
//            int totalTimeout = policy.totalTimeout;
//            int iteration = 0;
//            int commandSentCounter = 0;
//            boolean isClientTimeout;
//
//            if (totalTimeout > 0) {
//                deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);
//
//                if (socketTimeout == 0 || socketTimeout > totalTimeout) {
//                    socketTimeout = totalTimeout;
//                }
//            }
//
//
//            // Execute command until successful, timed out or maximum iterations have been reached.
//            long overallStart = System.nanoTime();
//            while (true) {
//                try {
//                    if (partition != null) {
//                        // Single record command node retrieval.
//                        node = getNode(cluster, partition, policy.replica, isRead);
//                    }
//                    Connection conn = node.getConnection(socketTimeout);
//
//                    try {
//                        // Set command buffer.
//                        writeBuffer();
//
//                        // Check if total timeout needs to be changed in send buffer.
//                        if (totalTimeout != policy.totalTimeout) {
//                            // Reset timeout in send buffer (destined for server) and socket.
//                            Buffer.intToBytes(totalTimeout, dataBuffer, 22);
//                        }
//
//                        // Send command.
//                        bytesSent += dataOffset;
//                        conn.write(dataBuffer, dataOffset);
//                        commandSentCounter++;
//
//                        // Parse results.
//                        parseResult(conn);
//
//                        // Put connection back in pool.
//                        node.putConnection(conn);
//                        ++rePooledConnections;
//
//                        // Command has completed successfully.  Exit method.
//                        stats.incrementCounter(M_OVERALL, DateTime.nanoTime() - overallStart, tags);
//                        return ResultCode.OK;
//                    }
//                    catch (AerospikeException ae) {
//                        LOGGER.debug("AS Ex", ae);
//                        if (ae.keepConnection()) {
//                            // Put connection back in pool.
//                            node.putConnection(conn);
//                            ++rePooledConnections;
//                        }
//                        else {
//                            // Close socket to flush out possible garbage.  Do not put back in pool.
//                            node.closeConnection(conn);
//                            ++closedNodeConnections;
//                        }
//
//                        if (ae.getResultCode() == ResultCode.TIMEOUT) {
//                            ++timeouts;
//                            // Go through retry logic on server timeout.
//                            LOGGER.warn("Server timeout: {}, {}, {}", node, sequence, iteration);
//                            exception = new AerospikeException.Timeout(node, policy, iteration + 1, false);
//                            isClientTimeout = false;
//
//                            if (isRead) {
//                                super.sequence++;
//                            }
//                        }
//                        else {
//                            ++exceptions;
//                            LOGGER.debug("Throw AerospikeException: {}, {}, {}, {}", node, sequence, iteration, ae.getResultCode());
//                            ae.setInDoubt(isRead, commandSentCounter);
//                            throw ae;
//                        }
//                    }
//                    catch (RuntimeException re) {
//                        ++exceptions;
//                        // All runtime exceptions are considered fatal.  Do not retry.
//                        // Close socket to flush out possible garbage.  Do not put back in pool.
//                        LOGGER.debug("Throw RuntimeException: {}, {}, {}", node, sequence, iteration);
//                        node.closeConnection(conn);
//                        throw re;
//                    }
//                    catch (SocketTimeoutException ste) {
//                        ++socketTimeouts;
//                        // Full timeout has been reached.
//                        LOGGER.warn("Socket timeout: {}, {}, {}", node, sequence, iteration);
//                        node.closeConnection(conn);
//                        isClientTimeout = true;
//
//                        if (isRead) {
//                            super.sequence++;
//                        }
//                    }
//                    catch (IOException ioe) {
//                        ++ioExceptions;
//                        // IO errors are considered temporary anomalies.  Retry.
//                        LOGGER.warn("IOException: {}, {}, {}", node, sequence, iteration);
//                        node.closeConnection(conn);
//                        exception = new AerospikeException(ioe);
//                        isClientTimeout = false;
//                        super.sequence++;
//                    }
//                }
//                catch (AerospikeException.Connection ce) {
//                    // Socket connection error has occurred. Retry.
//                    LOGGER.warn("Connection error: {}, {}, {}", node, sequence, iteration);
//                    exception = ce;
//                    isClientTimeout = false;
//                    super.sequence++;
//                }
//
//                // Check maxRetries.
//                if (++iteration > policy.maxRetries) {
//                    break;
//                }
//                ++retries;
//
//                if (policy.totalTimeout > 0) {
//                    // Check for total timeout.
//                    long remaining = deadline - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);
//
//                    if (remaining <= 0) {
//                        break;
//                    }
//
//                    // Convert back to milliseconds for remaining check.
//                    remaining = TimeUnit.NANOSECONDS.toMillis(remaining);
//
//                    if (remaining < totalTimeout) {
//                        totalTimeout = (int)remaining;
//
//                        if (socketTimeout > totalTimeout) {
//                            socketTimeout = totalTimeout;
//                        }
//                    }
//                }
//
//                if (!isClientTimeout && policy.sleepBetweenRetries > 0) {
//                    // Sleep before trying again.
//                    Util.sleep(policy.sleepBetweenRetries);
//                }
//            }
//
//            // Retries have been exhausted.  Throw last exception.
//            if (isClientTimeout) {
//                LOGGER.warn("SocketTimeoutException: {}, {}", sequence, iteration);
//                //exception = new AerospikeException.Timeout(node, policy, iteration, true);
//                ++timeouts;
//                stats.addTime(M_OVERALL, DateTime.nanoTime() - overallStart, ChronoUnit.NANOS, tags);
//                return ResultCode.TIMEOUT;
//            }
//
//            LOGGER.debug(String.format("Runtime exception: %s, %d, %d", node.toString(), sequence, iteration), exception);
//            exception.setInDoubt(isRead, commandSentCounter);
//            ++exceptions;
//            throw exception;
//        }
//
//        @Override
//        protected void parseResult(final Connection conn) throws AerospikeException, IOException {
//            // Read header.
//            long start = System.nanoTime();
//            conn.readFully(dataBuffer, MSG_TOTAL_HEADER_SIZE);
//            long timeTaken = System.nanoTime() - start;
//            stats.addTime(M_TCP, timeTaken, ChronoUnit.NANOS, tags);
//            bytesRead += MSG_TOTAL_HEADER_SIZE;
//
//            int resultCode = dataBuffer[13] & 0xFF;
//            if (resultCode != 0) {
//                if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR || resultCode == ResultCode.LARGE_ITEM_NOT_FOUND) {
//                    // no-op;
//                    recordIterator.reset(resultCode);
//                    bytesDiscarded += emptySocket2(conn);
//                    return;
//                }
//                // TODO - UDF errors if we use em.
//                if (policy instanceof WritePolicy) {
//                    recordIterator.reset(ResultCode.OK);
//                    bytesDiscarded += emptySocket2(conn);
//                }
//                return;
//            }
//
//            if (policy instanceof WritePolicy) {
//                // don't care about the records and don't want em.
//                recordIterator.reset(ResultCode.OK);
//                bytesDiscarded += emptySocket2(conn);
//                return;
//            }
//
//            // keep parsing
//            long sz = Buffer.bytesToLong(dataBuffer, 0);
//            byte headerLength = dataBuffer[8];
//            int generation = Buffer.bytesToInt(dataBuffer, 14);
//            int expiration = Buffer.bytesToInt(dataBuffer, 18);
//            int fieldCount = Buffer.bytesToShort(dataBuffer, 26); // almost certainly 0
//            int opCount = Buffer.bytesToShort(dataBuffer, 28);
//            int receiveSize = ((int) (sz & 0xFFFFFFFFFFFFL)) - headerLength;
//
//            if (receiveSize > 0) {
//                sizeBuffer(receiveSize);
//                conn.readFully(dataBuffer, receiveSize);
//                bytesRead += receiveSize;
//            }
//
//            if (opCount == 0) {
//                // Bin data was not returned.
//                recordIterator.reset(ResultCode.OK);
//                return;
//            }
//            recordIterator.parseRecord(opCount, fieldCount, generation, expiration, dataBuffer);
//        }
//
//        protected int emptySocket2(Connection conn) throws IOException {
//            // There should not be any more bytes.
//            // Empty the socket to be safe.
//            long sz = Buffer.bytesToLong(dataBuffer, 0);
//            int headerLength = dataBuffer[8];
//            int receiveSize = ((int)(sz & 0xFFFFFFFFFFFFL)) - headerLength;
//
//            // Read remaining message bytes.
//            if (receiveSize > 0)
//            {
//                sizeBuffer(receiveSize);
//                conn.readFully(dataBuffer, receiveSize);
//            }
//            return receiveSize;
//        }
//    }
//
//    /**
//     * Ugly but it's a way to avoid lookups and writes in the Ultrabrew tables on every client
//     * interaction.
//     */
//    class MetricFlusher implements Runnable {
//
//        // Interactions
//        private long prevRawWrites;
//        private long prevRawReads;
//        private long prevMapWrites;
//        private long prevMapReads;
//
//        // AS IO
//        private long prevTimeouts;
//        private long prevExceptions;
//        private long prevRetries;
//        private long prevIoExceptions;
//        private long prevSocketTimeouts;
//        private long prevClosedNodeConnection;
//        private long prevRePooledConnection;
//        private long prevBytesSent;
//        private long prevBytesRead;
//        private long prevBytesDiscarded;
//
//        @Override
//        public void run() {
//            prevRawWrites = updateCounter(ASSyncClient.this.rawWrites, prevRawWrites, M_RAW_WRITES);
//            prevRawReads = updateCounter(ASSyncClient.this.rawReads, prevRawReads, M_RAW_READS);
//            prevMapWrites = updateCounter(ASSyncClient.this.mapWrites, prevMapWrites, M_MAP_WRITES);
//            prevMapReads = updateCounter(ASSyncClient.this.mapReads, prevMapReads, M_MAP_READS);
//
//            prevTimeouts = updateCounter(ASSyncClient.this.timeouts, prevTimeouts, M_IO_TIMEOUTS);
//            prevExceptions = updateCounter(ASSyncClient.this.exceptions, prevExceptions, M_IO_EX);
//            prevRetries = updateCounter(ASSyncClient.this.retries, prevRetries, M_IO_RETRIES);
//            prevIoExceptions = updateCounter(ASSyncClient.this.ioExceptions, prevIoExceptions, M_IO_IOEX);
//            prevSocketTimeouts = updateCounter(ASSyncClient.this.socketTimeouts, prevSocketTimeouts, M_IO_SOT);
//            prevClosedNodeConnection = updateCounter(ASSyncClient.this.closedNodeConnections, prevClosedNodeConnection, M_IO_CLOSED);
//            prevRePooledConnection = updateCounter(ASSyncClient.this.rePooledConnections, prevRePooledConnection, M_IO_REPOOLED);
//            prevBytesSent = updateCounter(ASSyncClient.this.bytesSent, prevBytesSent, M_IO_BYTES_SENT);
//            prevBytesRead = updateCounter(ASSyncClient.this.bytesRead, prevBytesRead, M_IO_BYTES_READ);
//            prevBytesDiscarded = updateCounter(ASSyncClient.this.bytesDiscarded, prevBytesDiscarded, M_IO_BYTES_SKIPPED);
//        }
//
//        long updateCounter(long latest, long previous, String metric) {
//            long delta = latest - previous;
//            stats.incrementCounter(metric, delta, tags);
//            return latest;
//        }
//    }
}

