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
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.FieldType;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ConsistencyLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;
import io.ultrabrew.metrics.Counter;
import io.ultrabrew.metrics.MetricRegistry;
import io.ultrabrew.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A client for making batch reads to the Aerospike cluster.
 *
 * Currently it will perform a synchronous read wherein the call to
 * {@link #batchGet(byte[][], int, byte[], byte[])} will map the keys to the proper
 * nodes, then fire a request to each node one by one. The idea is that the caller
 * will have multiple threads calling into AS so we don't need to spin up threads
 * at this level.
 *
 * The response will hold a buffer of the matched records that can be iterated
 * on. Note that unless an exception happens (timeout or whatnot) the result code
 * will always be OK, even if no records were matched in the request. In that
 * case the iterator wouldn't have any data.
 *
 * TODO - Do we need to read buffer on error or would it reset?
 */
public class ASBatchClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ASBatchClient.class);
  private static final int MAX_BUFFER_SIZE = 1024 * 1024 * 10;  // 10 MB

  /** The cluster we're working with. */
  private final Cluster cluster;

  /** The namespace this client works on. */
  private final String namespace;

  /** Where we're reporting metrics. */
  private final MetricRegistry metricRegistry;

  /** Hashers used to digest the keys. */
  private final ThreadLocal<UpdatingRipeMD160> hashers;

  /** Response iterators. */
  private final ThreadLocal<BatchRecordIterator> bris;

  /** A pool of commands for each thread. Each array can grow to the number of
   * nodes in the cluster. */
  private final ThreadLocal<BatchASCommand[]> commands;

  /** Default batch policy. */
  private final BatchPolicy defaultBatchPolicy;

  /** Metric tags. */
  private final String[] tags;

  private ReusablePartition partition;

  /** Metrics. */
  private volatile long batchReads;
  private volatile long noNodeForKey;

  private volatile long timeouts;
  private volatile long exceptions;
  private volatile long retries;
  private volatile long ioExceptions;
  private volatile long socketTimeouts;
  private volatile long closedNodeConnections;
  private volatile long rePooledConnections;
  private volatile long bytesSent;
  private volatile long bytesRead;
  private volatile long bytesDiscarded;

  private volatile Timer callTimer;
  private volatile Timer overallTimer;
  private volatile Timer timeToFirstByteTimer;
  private Timer nodesPerBatch; // not really a timer
  private Timer keysPerNodePerBatch; // not really a timer
  private Timer hitsPerNode; // not really a timer

  /**
   Default ctor.
   * @param cluster The non-null cluster to work with.
   * @param namespace The non-null namespace to write to.
   * @param metricRegistry The metric registry to write metrics to.
   * @param executorService A thread to schedule writes to the registry for
   *                        high-perf counters, etc.
   */
  public ASBatchClient(final Cluster cluster,
                       final String namespace,
                       final MetricRegistry metricRegistry,
                       final ScheduledExecutorService executorService) {
    this.cluster = cluster;
    this.namespace = namespace;
    this.metricRegistry = metricRegistry;
    defaultBatchPolicy = new BatchPolicy();
    defaultBatchPolicy.sendSetName = true;
    tags = new String[] { "namespace", namespace, "callType", "batchSync" };
    executorService.scheduleAtFixedRate(new MetricFlusher(),
            TimeUnit.MINUTES.toMillis(1),
            TimeUnit.MINUTES.toMillis(1),
            TimeUnit.MILLISECONDS);
    partition = new ReusablePartition(namespace);
    hashers = ThreadLocal.withInitial(() -> { return new UpdatingRipeMD160(); });
    bris = ThreadLocal.withInitial(() -> { return new BatchRecordIterator(); });
    commands = ThreadLocal.withInitial(() -> {
      // dunno if we have the number of nodes off the bat or if it's populated
      // as the client interacts.
      final int numNodes = Math.max(8, cluster.getNodes().length);
      final BatchASCommand[] cmds = new BatchASCommand[numNodes];
      for (int i = 0; i < cmds.length; i++) {
        cmds[i] = new BatchASCommand();
      }
      return cmds;
    });

    callTimer = metricRegistry.timer("AerospikeClient.tcp.call.latency");
    overallTimer = metricRegistry.timer("AerospikeClient.overall.call.latency");
    timeToFirstByteTimer = metricRegistry.timer("AerospikeClient.tcp.timeToFirstByte.latency");
    nodesPerBatch = metricRegistry.timer("AerospikeClient.batch.nodesPerBatch");
    keysPerNodePerBatch = metricRegistry.timer("AerospikeClient.batch.keysPerNodePerBatch");
    hitsPerNode = metricRegistry.timer("AerospikeClient.batch.hitsPerNode");
  }

  /**
   * Routes each key to the node that should serve it, batching keys to particular
   * nodes. This is a synchronous call so each node is called one after the other.
   * The result will contain any matching records. May be empty with an OK result.
   *
   * <b>WARNING:</b> Do not pass the result to different threads. It's a thread
   * local.
   *
   * @param keys A non-null array of 1 or more non-null keys.
   * @param keyLength The number of keys to read from the keys array (for re-use).
   * @param setBytes Optional set to read from.
   * @param bin Optional bin to read from.
   * @return A non-null batch record iterator. Note this will throw if there is
   * an exception.
   */
  public BatchRecordIterator batchGet(final byte[][] keys,
                                      final int keyLength,
                                      final byte[] setBytes,
                                      final byte[] bin) {
    // TODO - find a better way to map nodes. Shouldn't be too bad for now.
    final Map<Node, BatchASCommand> batches = new HashMap<Node, BatchASCommand>();
    final BatchRecordIterator bri = bris.get();
    bri.reset();
    BatchASCommand[] cmds = commands.get();
    int cmdsIdx = 0;

    // here we hash each key and map it to a node.
    final UpdatingRipeMD160 hasher = hashers.get();
    for (int i = 0; i < keyLength; i++) {
      final byte[] key = keys[i];
      hasher.update(setBytes, 0, setBytes.length);
      hasher.update((byte) ParticleType.BLOB);
      hasher.update(key, 0, key.length);
      final byte[] digest = hasher.digest(); // updates in the ByteValue

      final int partitionId = (Buffer.littleBytesToInt(hasher.getDigest(), 0) & 0xFFFF) % Node.PARTITIONS;
      // TODO - find a way around this... grr.
      partition.update(partitionId);
      final Node node = cluster.getMasterNode(partition.get());
      if (node == null) {
        throw new IllegalStateException("No node for partition ID: " + partitionId);
      }
      BatchASCommand batchReq = batches.get(node);
      if (batchReq == null) {
        if (cmdsIdx >= cmds.length) {
          // resize
          cmds = resizeCommands();
        }
        batchReq = cmds[cmdsIdx++];
        batchReq.reset(keys, node, bri, setBytes, bin);
        batches.put(node, batchReq);
      }
      batchReq.addkey(i, digest);
    }
    nodesPerBatch.update(batches.size(), tags);

    for (Map.Entry<Node, BatchASCommand> entry : batches.entrySet()) {
      final BatchASCommand command = entry.getValue();
      keysPerNodePerBatch.update(command.keyCount, tags);
      command.executeLocal(cluster, defaultBatchPolicy, entry.getKey());
    }
    return bri;
  }

  /**
   * Resizes the thread local command arrays.
   * @return The resized array.
   */
  private BatchASCommand[] resizeCommands() {
    BatchASCommand[] cmds = commands.get();
    BatchASCommand[] temp = new BatchASCommand[Math.max(cmds.length + 1, cluster.getNodes().length)];
    System.arraycopy(cmds, 0, temp, 0, cmds.length);
    commands.set(temp);
    return temp;
  }

  /**
   * A batch command for a particular command. This is responsible for collating
   * the keys for the node and firing off the request.
   */
  private class BatchASCommand extends MultiCommand {
    /** This sticks around and will grow to the max number of keys given. keyCount
        indexes into this array. */
    protected byte[][] digests;

    /** The buffer we're using to read off the socket. */
    protected InputStream bis;

    /** Read settings. */
    protected int readAttr = Command.INFO1_READ | Command.INFO1_GET_ALL;

    /** Digest used when parsing the response.. */
    protected byte[] responseDigest = new byte[DIGEST_SIZE];

    /** Iterator to store the particles in. */
    protected BatchRecordIterator bri;

    /** The ref to overall batch keys, not just for this node. */
    protected byte[][] keys;

    /** The number of keys we'll read. */
    protected int keyCount;

    /** The optional set to read. */
    protected byte[] setBytes;

    /** The optional bin to read. NOT USED RIGHT NOW */
    protected byte[] bin;

    /** An array of offsets into the keys index for matching responses to the
     * proper key. */
    protected int[] offsets;

    /** The node to write to. */
    protected Node node;

    /**
     * Default ctor.
     */
    BatchASCommand() {
      super(false);
      digests = new byte[16][];
      for (int i = 0; i < 16; i++) {
        digests[i] = new byte[DIGEST_SIZE];
      }
      offsets = new int[16];
    }

    /**
     * Resets the state of this entry.
     * @param keys The full keys reference of the overall batch.
     * @param node The node this batch will go to.
     * @param bri The iterator to populate with results.
     * @param setBytes Optional set to query.
     * @param bin Optional bin to query.
     */
    void reset(final byte[][] keys,
               final Node node,
               final BatchRecordIterator bri,
               final byte[] setBytes,
               final byte[] bin) {
      this.bri = bri;
      this.keys = keys;
      this.setBytes = setBytes;
      this.bin = bin;
      this.node = node;

      keyCount = 0;
    }

    /**
     * Adds a key/digest to the command.
     * @param idx The index into the overall batch keys array.
     * @param digest The digest of the key.
     */
    void addkey(final int idx, final byte[] digest) {
      if (keyCount + 1 >= offsets.length) {
        int[] temp = new int[offsets.length * 2];
        System.arraycopy(offsets, 0, temp, 0, keyCount);
        offsets = temp;

        byte[][] tempDigests = new byte[digests.length * 2][];
        System.arraycopy(digests, 0, tempDigests, 0, keyCount);
        digests = tempDigests;
      }

      offsets[keyCount] = idx;
      if (digests[keyCount] == null) {
        // cause we have to re-use the hasher's byte array. Should be quick though
        // as it's only 20 bytes.
        digests[keyCount] = Arrays.copyOf(digest, digest.length);
      } else {
        // cause we have to re-use the hasher's byte array. Should be quick though
        // as it's only 20 bytes.
        System.arraycopy(digest, 0, digests[keyCount], 0, digest.length);
      }
      keyCount++;
    }

    @Override
    protected void writeBuffer() {
      // cribbed from MultiCommand.java
      final int fieldCount = defaultBatchPolicy.sendSetName ? 2 : 1;
      int operationCount = 0;

      // Estimate buffer size.
      begin();
      dataOffset += FIELD_HEADER_SIZE + 5;

      dataOffset += keyCount * (DIGEST_SIZE + 5);
      dataOffset += Buffer.estimateSizeUtf8(namespace) + FIELD_HEADER_SIZE + 6;

      if (setBytes != null) {
        dataOffset += setBytes.length + FIELD_HEADER_SIZE;
      }

      if (bin != null) {
        dataOffset += bin.length + OPERATION_HEADER_SIZE;
      }

      sizeBuffer();

      if (defaultBatchPolicy.consistencyLevel == ConsistencyLevel.CONSISTENCY_ALL) {
        readAttr |= Command.INFO1_CONSISTENCY_ALL;
      }

      writeHeader(defaultBatchPolicy, readAttr | Command.INFO1_BATCH, 0, 1, 0);
      int fieldSizeOffset = dataOffset;
      writeFieldHeader(0, defaultBatchPolicy.sendSetName && setBytes != null ? FieldType.BATCH_INDEX_WITH_SET : FieldType.BATCH_INDEX);  // Need to update size at end

      Buffer.intToBytes(keyCount, dataBuffer, dataOffset);
      dataOffset += 4;
      dataBuffer[dataOffset++] = (defaultBatchPolicy.allowInline)? (byte)1 : (byte)0;

      for (int i = 0; i < keyCount; i++) {
        int index = offsets[i];
        Buffer.intToBytes(index, dataBuffer, dataOffset);
        dataOffset += 4;

        byte[] digest = digests[i];
        System.arraycopy(digest, 0, dataBuffer, dataOffset, DIGEST_SIZE);
        dataOffset += digest.length;

        if (i == 0) {
          // dump namespace and set name, otherwise we skip
          dataBuffer[dataOffset++] = 0;  // do not repeat
          dataBuffer[dataOffset++] = (byte) readAttr;

          Buffer.shortToBytes(fieldCount, dataBuffer, dataOffset);
          dataOffset += 2;
          Buffer.shortToBytes(operationCount/* operationCount COMES FROM BinNames[] length in the original. Does that have to match key count? */,
                  dataBuffer, dataOffset);
          dataOffset += 2;
          writeField(ASBatchClient.this.namespace, FieldType.NAMESPACE);

          if (defaultBatchPolicy.sendSetName && setBytes != null) {
            writeField(setBytes, FieldType.TABLE);
          }

//          if (bin != null) {
//            System.arraycopy(bin, 0, dataBuffer, dataOffset + OPERATION_HEADER_SIZE, bin.length);
//            Buffer.intToBytes(bin.length + 4, dataBuffer, dataOffset);
//            dataOffset += 4;
//            dataBuffer[dataOffset++] = (byte) Operation.Type.READ.protocolType;
//            dataBuffer[dataOffset++] = (byte) 0;
//            dataBuffer[dataOffset++] = (byte) 0;
//            dataBuffer[dataOffset++] = (byte) bin.length;
//            dataOffset += bin.length;
//          }
        } else {
          dataBuffer[dataOffset++] = 1;  // repeat
        }
      }

      // Write real field size.
      Buffer.intToBytes(dataOffset - MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
      end();
    }

    /**
     * Cribbed from the super class. All of this to avoid allocations in the
     * {@link Partition} class (which we still have to instantiate since the fields
     * are final but at least it's small).
     * @param cluster The non-null cluster.
     * @param policy The non-null policy.
     * @param node Optional node to write to.
     * @return The {@link ResultCode}.
     */
    public int executeLocal(final Cluster cluster,
                            final Policy policy,
                            final Node node) {
      AerospikeException exception = null;
      long deadline = 0;
      int socketTimeout = policy.socketTimeout;
      int totalTimeout = policy.totalTimeout;
      int iteration = 0;
      int commandSentCounter = 0;
      boolean isClientTimeout;

      if (totalTimeout > 0) {
        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);

        if (socketTimeout == 0 || socketTimeout > totalTimeout) {
          socketTimeout = totalTimeout;
        }
      }

      // Execute command until successful, timed out or maximum iterations have been reached.
      long overallStart = System.nanoTime();
      while (true) {
        try {
          Connection conn = node.getConnection(socketTimeout);

          try {
            // Set command buffer.
            writeBuffer();

            // Check if total timeout needs to be changed in send buffer.
            if (totalTimeout != policy.totalTimeout) {
              // Reset timeout in send buffer (destined for server) and socket.
              Buffer.intToBytes(totalTimeout, dataBuffer, 22);
            }

            // Send command.
            bytesSent += dataOffset;
            conn.write(dataBuffer, dataOffset);
            commandSentCounter++;

            // Parse results.
            parseResultLocal(conn);

            // Put connection back in pool.
            node.putConnection(conn);
            ++rePooledConnections;

            // Command has completed successfully.  Exit method.
            overallTimer.update(System.nanoTime() - overallStart, tags);
            return ResultCode.OK;
          }
          catch (AerospikeException ae) {
            LOGGER.debug("AS Ex", ae);
            if (ae.keepConnection()) {
              // Put connection back in pool.
              node.putConnection(conn);
              ++rePooledConnections;
            }
            else {
              // Close socket to flush out possible garbage.  Do not put back in pool.
              node.closeConnection(conn);
              ++closedNodeConnections;
            }

            if (ae.getResultCode() == ResultCode.TIMEOUT) {
              ++timeouts;
              // Go through retry logic on server timeout.
              LOGGER.warn("Server timeout: {}, {}, {}", node, sequence, iteration);
              exception = new AerospikeException.Timeout(node, policy, iteration + 1, false);
              isClientTimeout = false;
              super.sequence++;
            }
            else {
              ++exceptions;
              LOGGER.debug("Throw AerospikeException: {}, {}, {}, {}", node, sequence, iteration, ae.getResultCode());
              ae.setInDoubt(true, commandSentCounter);
              throw ae;
            }
          }
          catch (RuntimeException re) {
            ++exceptions;
            // All runtime exceptions are considered fatal.  Do not retry.
            // Close socket to flush out possible garbage.  Do not put back in pool.
            LOGGER.debug("Throw RuntimeException: {}, {}, {}", node, sequence, iteration);
            node.closeConnection(conn);
            throw re;
          }
          catch (SocketTimeoutException ste) {
            ++socketTimeouts;
            // Full timeout has been reached.
            LOGGER.warn("Socket timeout: {}, {}, {}", node, sequence, iteration);
            node.closeConnection(conn);
            isClientTimeout = true;
            super.sequence++;
          }
          catch (IOException ioe) {
            ++ioExceptions;
            // IO errors are considered temporary anomalies.  Retry.
            LOGGER.warn("IOException: {}, {}, {}", node, sequence, iteration);
            node.closeConnection(conn);
            exception = new AerospikeException(ioe);
            isClientTimeout = false;
            super.sequence++;
          }
        }
        catch (AerospikeException.Connection ce) {
          // Socket connection error has occurred. Retry.
          LOGGER.warn("Connection error: {}, {}, {}", node, sequence, iteration);
          exception = ce;
          isClientTimeout = false;
          super.sequence++;
        }

        // Check maxRetries.
        if (++iteration > policy.maxRetries) {
          break;
        }
        ++retries;

        if (policy.totalTimeout > 0) {
          // Check for total timeout.
          long remaining = deadline - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);

          if (remaining <= 0) {
            break;
          }

          // Convert back to milliseconds for remaining check.
          remaining = TimeUnit.NANOSECONDS.toMillis(remaining);

          if (remaining < totalTimeout) {
            totalTimeout = (int)remaining;

            if (socketTimeout > totalTimeout) {
              socketTimeout = totalTimeout;
            }
          }
        }

        if (!isClientTimeout && policy.sleepBetweenRetries > 0) {
          // Sleep before trying again.
          Util.sleep(policy.sleepBetweenRetries);
        }
      }

      // Retries have been exhausted.  Throw last exception.
      if (isClientTimeout) {
        LOGGER.warn("SocketTimeoutException: {}, {}", sequence, iteration);
        ++timeouts;
        overallTimer.update(System.nanoTime() - overallStart, tags);
        return ResultCode.TIMEOUT;
      }

      LOGGER.debug(String.format("Runtime exception: %s, %d, %d", node.toString(), sequence, iteration), exception);
      exception.setInDoubt(true, commandSentCounter);
      ++exceptions;
      throw exception;
    }

    /**
     * Does what it says on the box. Parses the connection, setting up the BIS.
     * @param conn The non-null connection to read from.
     * @throws IOException If something goes pear shaped.
     */
    protected void parseResultLocal(final Connection conn) throws IOException {
      // cribbed from MultiCommand
      // Read socket into receive buffer one record at a time.  Do not read entire receive size
      // because the thread local receive buffer would be too big.  Also, scan callbacks can nest
      // further database commands which contend with the receive buffer.
      bis = conn.getInputStream();
      long start = System.nanoTime();
      long ttfb = -1;
      boolean status = true;
      while (status) {
        // Read header. (reads to the tail of the buffer since data offset is the len of
        // the sent message?
        readBytesLocal(8);
        if (ttfb < 0) {
          ttfb = System.nanoTime() - start;
          timeToFirstByteTimer.update(ttfb, tags);
        }

        long size = Buffer.bytesToLong(dataBuffer, 0);
        // NOTE This is a concatenation of a few flags, not just the size of the
        // total response.
        int receiveSize = ((int) (size & 0xFFFFFFFFFFFFL));
        if (receiveSize > 0) {
          status = parseGroup(receiveSize);
        }
      }
      long timeTaken = System.nanoTime() - start;
      callTimer.update(timeTaken, tags);
    }

    /**
     * Override that reads a chunk of data into the data buffer.
     * @param length The length to read.
     * @throws IOException If something goes pear shaped.
     */
    protected final void readBytesLocal(int length) throws IOException {
      // cribbed from MultiCommand
      if (length > dataBuffer.length) {
        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if (length > MAX_BUFFER_SIZE) {
          throw new IllegalArgumentException("Invalid readBytes length: " + length);
        }
        dataBuffer = new byte[length];
      }

      int pos = 0;

      while (pos < length) {
        int count = bis.read(dataBuffer, pos, length - pos);

        if (count < 0) {
          throw new EOFException();
        }
        pos += count;
      }
      bytesRead += length;
      dataOffset += length;
    }

    /**
     * Parses records for a "group" which is predicated on the namespace I think.
     * @param receiveSize The receive size flags.
     * @return True if we read something, false if not.
     * @throws IOException If something goes pear shaped.
     */
    protected boolean parseGroup(int receiveSize) throws IOException {
      // cribbed from MultiCommand
      // Parse each message response and add it to the result array
      dataOffset = 0;

      // recieveSize is the concatenated values in a long. When debugging it
      // looks huge.
      // DataOffset starts at 0, natch, and keeps growing as we read out
      // data into the buffer. BUT something triggers the dataOffset to
      // equal the receiveSize when we're done. So look for that.
      int hits = 0;
      while (dataOffset < receiveSize) {
        readBytesLocal(MSG_REMAINING_HEADER_SIZE);
        resultCode = dataBuffer[5] & 0xFF;
        bri.setResultCode(resultCode);

        // The only valid server return codes are "ok" and "not found".
        // If other return codes are received, then abort the batch.
        if (resultCode != 0) {
          if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
            //if (stopOnNotFound) {
            //  return false;
            //}
          }
          else {
            throw new AerospikeException(resultCode);
          }
        }

        byte info3 = dataBuffer[3];
        // If this is the end marker of the response, do not proceed further
        if ((info3 & Command.INFO3_LAST) == Command.INFO3_LAST) {
          return false;
        }

        //generation = Buffer.bytesToInt(dataBuffer, 6);
        //expiration = Buffer.bytesToInt(dataBuffer, 10);
        batchIndex = Buffer.bytesToInt(dataBuffer, 14);
        fieldCount = Buffer.bytesToShort(dataBuffer, 18);
        //opCount = Buffer.bytesToShort(dataBuffer, 20);

        parseKeyLocal(fieldCount);
        parseRow(batchIndex);
      }

      ++hits;
      hitsPerNode.update(hits, tags);
      return true;
    }

    /**
     * Reads the key.
     * @param fieldCount The number of fields expected
     * @return The index into the digest offset if successful, -1 if not matched.
     * @throws AerospikeException If something goes pear shaped.
     * @throws IOException If something goes pear shaped.
     */
    protected final void parseKeyLocal(int fieldCount) throws AerospikeException, IOException {
      // cribbed from MultiCommand
      for (int i = 0; i < fieldCount; i++) {
        readBytesLocal(4);
        int fieldlen = Buffer.bytesToInt(dataBuffer, 0);
        readBytesLocal(fieldlen);
        int fieldtype = dataBuffer[0];
        int size = fieldlen - 1;

        switch (fieldtype) {
          case FieldType.DIGEST_RIPE:
            if (size != DIGEST_SIZE) {
              throw new IllegalStateException("Digest size was " + size + " when it _SHOULD_ be " + DIGEST_SIZE);
            }
            //System.arraycopy(dataBuffer, 1, responseDigest, 0, size);
            break;

          case FieldType.NAMESPACE:
            // don't need it
            //namespace = Buffer.utf8ToString(dataBuffer, 1, size);
            break;

          case FieldType.TABLE:
            // don't need it
            //setName = Buffer.utf8ToString(dataBuffer, 1, size);
            break;

          case FieldType.KEY:
            // don't need it
            //userKey = Buffer.bytesToKeyValue(dataBuffer[1], dataBuffer, 2, size-1);
            break;
        }
      }
    }

    /**
     * Hacked up to redirect just the particle payload to the BRI.
     * @param batchIndex The index of the record in the original batch key array.
     * @throws IOException If something goes pear shaped.
     */
    protected void parseRow(final int batchIndex) throws IOException {
      // comes from MultiCommand#parseRecord();
      if (resultCode == 0) {
        // one bin only
        readBytesLocal(8);

        int opSize = Buffer.bytesToInt(dataBuffer, 0);
        byte particleType = dataBuffer[5];
        byte nameSize = dataBuffer[7];

        // don't need the name
        readBytesLocal(nameSize);
        int particleBytesSize = (int) (opSize - (4 + nameSize));

        // DO need the data
        int read = bri.read(batchIndex, bis, particleType, particleBytesSize);
        dataOffset += read;
        bytesRead += read;
        // left for debugging.
        //int aryIDx = bri.writeIndex - 1;
        //Object value = Buffer.bytesToParticle(bri.particleType[aryIDx],
        //        bri.buffer,
        //        bri.particleIndex[aryIDx],
        //        bri.particleLength[aryIDx]);
      }
    }

    @Override
    protected void parseRow(final Key key) throws IOException {
      // shouldn't be made.
      throw new UnsupportedOperationException();
    }

  }

  /**
   * Ugly but it's a way to avoid lookups and writes in the Ultrabrew tables on every client
   * interaction.
   */
  class MetricFlusher implements Runnable {
    // Interactions
    private final Counter batchReads;
    private final Counter noNodeForKey;

    // AS IO
    private final Counter timeouts;
    private final Counter exceptions;
    private final Counter retries;
    private final Counter ioExceptions;
    private final Counter socketTimeouts;
    private final Counter closedNodeConnections;
    private final Counter rePooledConnections;
    private final Counter bytesSent;
    private final Counter bytesRead;
    private final Counter bytesDiscarded;

    // Interactions
    private long prevBatchReads;
    private long prevNoNodeForKey;

    // AS IO
    private long prevTimeouts;
    private long prevExceptions;
    private long prevRetries;
    private long prevIoExceptions;
    private long prevSocketTimeouts;
    private long prevClosedNodeConnection;
    private long prevRePooledConnection;
    private long prevBytesSent;
    private long prevBytesRead;
    private long prevBytesDiscarded;


    MetricFlusher() {
      batchReads = metricRegistry.counter("AerospikeClient.batch.reads");
      noNodeForKey = metricRegistry.counter("AerospikeClient.batch.noNodeForKey");

      timeouts = metricRegistry.counter("AerospikeClient.io.timeouts");
      exceptions = metricRegistry.counter("AerospikeClient.io.exceptions");
      retries = metricRegistry.counter("AerospikeClient.io.retries");
      ioExceptions = metricRegistry.counter("AerospikeClient.io.ioExceptions");
      socketTimeouts = metricRegistry.counter("AerospikeClient.io.socketTimeout");
      closedNodeConnections = metricRegistry.counter("AerospikeClient.io.closedNodeConnections");
      rePooledConnections = metricRegistry.counter("AerospikeClient.io.rePooledConnections");
      bytesSent = metricRegistry.counter("AerospikeClient.io.bytesSent");
      bytesRead = metricRegistry.counter("AerospikeClient.io.bytesRead");
      bytesDiscarded = metricRegistry.counter("AerospikeClient.io.bytesSkipped");
    }

    @Override
    public void run() {
      prevBatchReads = updateCounter(ASBatchClient.this.batchReads, prevBatchReads, batchReads);
      prevNoNodeForKey = updateCounter(ASBatchClient.this.noNodeForKey, prevNoNodeForKey, noNodeForKey);

      prevTimeouts = updateCounter(ASBatchClient.this.timeouts, prevTimeouts, timeouts);
      prevExceptions = updateCounter(ASBatchClient.this.exceptions, prevExceptions, exceptions);
      prevRetries = updateCounter(ASBatchClient.this.retries, prevRetries, retries);
      prevIoExceptions = updateCounter(ASBatchClient.this.ioExceptions, prevIoExceptions, ioExceptions);
      prevSocketTimeouts = updateCounter(ASBatchClient.this.socketTimeouts, prevSocketTimeouts, socketTimeouts);
      prevClosedNodeConnection = updateCounter(ASBatchClient.this.closedNodeConnections, prevClosedNodeConnection, closedNodeConnections);
      prevRePooledConnection = updateCounter(ASBatchClient.this.rePooledConnections, prevRePooledConnection, rePooledConnections);
      prevBytesSent = updateCounter(ASBatchClient.this.bytesSent, prevBytesSent, bytesSent);
      prevBytesRead = updateCounter(ASBatchClient.this.bytesRead, prevBytesRead, bytesRead);
      prevBytesDiscarded = updateCounter(ASBatchClient.this.bytesDiscarded, prevBytesDiscarded, bytesDiscarded);
    }

    long updateCounter(long latest, long previous, Counter counter) {
      long delta = latest - previous;
      counter.inc(delta, tags);
      return latest;
    }
  }
}
