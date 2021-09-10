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
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.util.Packer;
import net.opentsdb.aura.metrics.core.downsample.AggregationLengthIterator;
import net.opentsdb.aura.metrics.core.downsample.DownSampledTimeSeriesEncoder;
import org.luaj.vm2.LuaValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DSEncodedValue extends Value {

  private static final Logger LOGGER = LoggerFactory.getLogger(DSEncodedValue.class);
  private static final int MAP_BEGIN_BYTES = 5;
  private DownSampledTimeSeriesEncoder encoder;

  private int size;
  private int recordOffset;
  private int headerLength;
  private int length;

  DSEncodedValue() {}

  public DSEncodedValue(DownSampledTimeSeriesEncoder encoder, int recordOffset) {
    reset(encoder, recordOffset);
  }

  public void reset(DownSampledTimeSeriesEncoder encoder, int recordOffset) {
    this.encoder = encoder;
    this.size = 1 + encoder.getAggCount(); // +1 for the header entry
    this.recordOffset = recordOffset;
  }

  @Override
  public int estimateSize() throws AerospikeException {

    int intervalCount = encoder.getIntervalCount();

    // header bytes
    headerLength = (int) (3 + Math.ceil(intervalCount / Byte.SIZE));

    int bytes = particleMetaBytes(headerLength) + headerLength;

    AggregationLengthIterator iterator = encoder.aggIterator();
    while (iterator.hasNext()) {
      iterator.next();
      int lengthInBytes = iterator.aggLengthInBytes();
      bytes += particleMetaBytes(lengthInBytes);
      bytes += lengthInBytes;
    }

    assert size < 15;
    bytes += MAP_BEGIN_BYTES; // to encode map begin
    bytes += size; // keys

    this.length = bytes;
    return bytes;
  }

  @Override
  public int write(byte[] buffer, int offset) throws AerospikeException {

    writeMapBegin(buffer, offset, size);
    offset += MAP_BEGIN_BYTES;

    buffer[offset++] = encodeMapKey(recordOffset, 0); // header

    int length = writeParticleMetaBytes(buffer, offset, headerLength);
    offset += length;
    encoder.serializeHeader(buffer, offset);
    offset += headerLength;

    AggregationLengthIterator iterator = encoder.aggIterator();
    while (iterator.hasNext()) {
      iterator.next();

      buffer[offset++] = encodeMapKey(recordOffset, iterator.aggOrdinal());
      int bytes = iterator.aggLengthInBytes();
      length = writeParticleMetaBytes(buffer, offset, bytes);
      offset += length;

      iterator.serialize(buffer, offset);
      offset += bytes;
    }
    return this.length;
  }

  private int particleMetaBytes(int size) {
    size++;

    int bytes;
    if (size < 32) {
      bytes = 1;
    } else if (size < 256) {
      bytes = 2;
    } else if (size < 65536) {
      bytes = 3;
    } else {
      bytes = 5;
    }

    bytes++; // for type ParticleType.BLOB

    return bytes;
  }

  private int writeParticleMetaBytes(byte[] buffer, int offset, int size) {
    size++;

    int bytes;
    if (size < 32) {
      buffer[offset++] = (byte) (0xa0 | size);
      bytes = 1;
    } else if (size < 256) {
      buffer[offset++] = (byte) (0xd9);
      buffer[offset++] = (byte) size;
      bytes = 2;
    } else if (size < 65536) {
      buffer[offset++] = (byte) (0xda);
      Buffer.shortToBytes(size, buffer, offset);
      offset += 2;
      bytes = 3;
    } else {
      buffer[offset++] = (byte) (0xdb);
      Buffer.intToBytes(size, buffer, offset);
      offset += 4;
      bytes = 5;
    }
    buffer[offset++] = (byte) ParticleType.BLOB;
    bytes++;
    return bytes;
  }

  private void writeMapBegin(byte[] buffer, int offset, int size) {
    //     assuming size is < 16
    buffer[offset++] = (byte) (0x80 | (size + 1)); // not sure why the +1
    buffer[offset++] = (byte) 0xc7;
    buffer[offset++] = (byte) 0;
    buffer[offset++] = (byte) MapOrder.KEY_ORDERED.attributes;
    buffer[offset++] = (byte) 0xc0;
  }

  private static final byte encodeMapKey(final int recordOffset, final int ordinal) {
    return (byte) (recordOffset << 4 | ordinal);
  }

  @Override
  public void pack(Packer packer) {}

  @Override
  public int getType() {
    return ParticleType.MAP;
  }

  @Override
  public Object getObject() {
    throw new UnsupportedOperationException();
  }

  @Override
  public LuaValue getLuaValue(LuaInstance instance) {
    throw new UnsupportedOperationException();
  }
}
