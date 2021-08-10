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

import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import net.opentsdb.aura.metrics.core.gorilla.OnHeapGorillaRawSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles parsing of an Aerospike map record. It's meant for re-use by
 * instantiating once and calling {@link #reset(byte[], int, int)} with the
 * index into an AS record buffer.
 * 
 * TODO - UTs for this class. For now it's working fine with our maps.
 */
public class AerospikeRecordMap {
  private static final Logger LOGGER = LoggerFactory.getLogger(AerospikeRecordMap.class);
  protected int size = 0;
  protected byte[] buffer;
  protected int len;
  protected int readIdx;
  protected int[] keys;
  protected int keysIdx;
  protected int[] offsets;
  protected int[] lengths;
  protected int readOut;

  void clear() {
    this.buffer = null;
    keysIdx = 0;
  }

  boolean isEmpty() {
    return keysIdx <= 0;
  }

  public void reset(byte[] buffer, int offset, int len) {
    this.buffer = buffer;
    this.len = len;
    readIdx = offset;
    if (keys == null) {
      keys = new int[3];
      offsets = new int[3];
      lengths = new int[3];
    }
    keysIdx = 0;
    readOut = 0;

    if (len <= 0) {
      size = 0;
      return;
    }

    // pulled from AS unpacker
    int type = buffer[readIdx++] & 0xFF;
    if ((type & 0xf0) == 0x80) {
      size = type & 0x0f;
    }
    else if (type == 0xde) {
      size = Buffer.bytesToShort(buffer, offset);
      readIdx += 2;
    }
    else if (type == 0xdf) {
      size = Buffer.bytesToInt(buffer, offset);
      readIdx += 4;
    } else {
      // nothing.
      LOGGER.info("Nothing for type? " + type);
      size = 0;
      return;
    }

    // fall-through to unpackMap()
    for (int i = 0; i < size; i++) {
      Integer key = unpackInteger();
      if (key == null) {
        readIdx++;
        continue;
      }

      if (keysIdx + 1 >= keys.length) {
        int[] temp = new int[keys.length * 2];
        System.arraycopy(keys, 0, temp, 0, keysIdx);
        keys = temp;

        temp = new int[keys.length * 2];
        System.arraycopy(offsets, 0, temp, 0, keysIdx);
        offsets = temp;

        temp = new int[keys.length * 2];
        System.arraycopy(lengths, 0, temp, 0, keysIdx);
        lengths = temp;
      }
      keys[keysIdx++] = key;

      Boolean read = unpackEntry();
      if (read == null) {
        LOGGER.info("WTF? Didn't read an entry?");
      }
    }

    if (keysIdx < 2) {
      return;
    }

    sort(0, keysIdx - 1);
  }

  public void encoder(final OnHeapGorillaRawSegment segment,
                      final int segmentTime,
                      final int idx) {
    int offset = offsets[idx];
    int length = lengths[idx];
    segment.reset(segmentTime, buffer, offset, length);
  }

  public int size() {
    return keysIdx;
  }

  public int[] keys() {
    return keys;
  }

  private Integer unpackInteger()  {
    int type = buffer[readIdx++] & 0xff;

    switch (type) {
      case 0xc0: { // nil
        return null;
      }

      case 0xc3: { // boolean true
        throw new IllegalStateException("Shouldn't be a bool");
        //return getBoolean(true);
      }

      case 0xc2: { // boolean false
        //return getBoolean(false);
        throw new IllegalStateException("Shouldn't be a bool");
      }

      case 0xca: { // float
//        float val = Float.intBitsToFloat(Buffer.bytesToInt(buffer, offset));
//        offset += 4;
//        return getDouble(val);
        throw new IllegalStateException("Shouldn't be a float");
      }

      case 0xcb: { // double
//        double val = Double.longBitsToDouble(Buffer.bytesToLong(buffer, offset));
//        offset += 8;
//        return getDouble(val);
        throw new IllegalStateException("Shouldn't be a double");
      }

      case 0xd0: { // signed 8 bit integer
        return (int) buffer[readIdx++];
      }

      case 0xcc: { // unsigned 8 bit integer
        return (int) buffer[readIdx++] & 0xff;
      }

      case 0xd1: { // signed 16 bit integer
        short val = Buffer.bigSigned16ToShort(buffer, readIdx);
        readIdx += 2;
        return (int) val;
      }

      case 0xcd: { // unsigned 16 bit integer
        int val = Buffer.bytesToShort(buffer, readIdx);
        readIdx += 2;
        return (int) val;
      }

      case 0xd2: { // signed 32 bit integer
        int val = Buffer.bytesToInt(buffer, readIdx);
        readIdx += 4;
        return (int) val;
      }

      case 0xce: { // unsigned 32 bit integer
        long val = Buffer.bigUnsigned32ToLong(buffer, readIdx);
        readIdx += 4;
        return (int) val;
      }

      case 0xd3: { // signed 64 bit integer
        long val = Buffer.bytesToLong(buffer, readIdx);
        readIdx += 8;
        return (int) val;
      }

      case 0xcf: { // unsigned 64 bit integer
        // Java is constrained to signed longs, so that is the best we can do here.
//        long val = Buffer.bytesToLong(buffer, offset);
//        offset += 8;
//        return getLong(val);
        throw new IllegalStateException("Shouldn't be an unsinged int");
      }

      case 0xc4:
      case 0xd9: { // string/raw bytes with 8 bit header
//        int count = buffer[offset++] & 0xff;
//        return (T)unpackBlob(count);
        throw new IllegalStateException("Shouldn't be a blob");
      }

      case 0xc5:
      case 0xda: { // string/raw bytes with 16 bit header
//        int count = Buffer.bytesToShort(buffer, offset);
//        offset += 2;
//        return (T)unpackBlob(count);
        throw new IllegalStateException("Shouldn't be a blob");
      }

      case 0xc6:
      case 0xdb: { // string/raw bytes with 32 bit header
        // Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
//        int count = Buffer.bytesToInt(buffer, offset);
//        offset += 4;
//        return (T)unpackBlob(count);
        throw new IllegalStateException("Shouldn't be a blob");
      }

      case 0xdc: { // list with 16 bit header
//        int count = Buffer.bytesToShort(buffer, offset);
//        offset += 2;
//        return unpackList(count);
        throw new IllegalStateException("Shouldn't be a list");
      }

      case 0xdd: { // list with 32 bit header
        // Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
//        int count = Buffer.bytesToInt(buffer, offset);
//        offset += 4;
//        return unpackList(count);
        throw new IllegalStateException("Shouldn't be a list");
      }

      case 0xde: { // map with 16 bit header
//        int count = Buffer.bytesToShort(buffer, offset);
//        offset += 2;
//        return unpackMap(count);
        throw new IllegalStateException("Shouldn't be a map");
      }

      case 0xdf: { // map with 32 bit header
//        // Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
//        int count = Buffer.bytesToInt(buffer, offset);
//        offset += 4;
//        return unpackMap(count);
        throw new IllegalStateException("Shouldn't be a map");
      }

      case 0xd4: { // Skip over type extension with 1 byte
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 1;
        return null;
      }

      case 0xd5: { // Skip over type extension with 2 bytes
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 2;
        return null;
      }

      case 0xd6: { // Skip over type extension with 4 bytes
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 4;
        return null;
      }

      case 0xd7: { // Skip over type extension with 8 bytes
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 8;
        return null;
      }

      case 0xd8: { // Skip over type extension with 16 bytes
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 16;
        return null;
      }

      case 0xc7: { // Skip over type extension with 8 bit header and bytes
        //LOGGER.warn("Type extension??");
        int count = buffer[readIdx] & 0xff;
        readIdx += count + 1 + 1;
        return null;
      }

      case 0xc8: { // Skip over type extension with 16 bit header and bytes
        //LOGGER.warn("Type extension??");
        int count = Buffer.bytesToShort(buffer, readIdx);
        readIdx += count + 1 + 2;
        return null;
      }

      case 0xc9: { // Skip over type extension with 32 bit header and bytes
        //LOGGER.warn("Type extension??");
        int count = Buffer.bytesToInt(buffer, readIdx);
        readIdx += count + 1 + 4;
        return null;
      }

      default: {
        if ((type & 0xe0) == 0xa0) { // raw bytes with 8 bit combined header
          //return unpackBlob(type & 0x1f);
          throw new IllegalStateException("Shouldn't be a blob");
        }

        if ((type & 0xf0) == 0x80) { // map with 8 bit combined header
          //return unpackMap(type & 0x0f);
          throw new IllegalStateException("Shouldn't be a map");
        }

        if ((type & 0xf0) == 0x90) { // list with 8 bit combined header
          //return unpackList(type & 0x0f);
          throw new IllegalStateException("Shouldn't be a list");
        }

        if (type < 0x80) { // 8 bit combined unsigned integer
          return type;
        }

        if (type >= 0xe0) { // 8 bit combined signed integer
          return type - 0xe0 - 32;
        }
        throw new IllegalStateException("Unknown unpack type: " + type);
      }
    }
  }

  private Boolean unpackEntry()  {
    int type = buffer[readIdx++] & 0xff;

    switch (type) {
      case 0xc0: { // nil
        return null;
      }

      case 0xc3: { // boolean true
        throw new IllegalStateException("Shouldn't be a bool");
        //return getBoolean(true);
      }

      case 0xc2: { // boolean false
        //return getBoolean(false);
        throw new IllegalStateException("Shouldn't be a bool");
      }

      case 0xca: { // float
//        float val = Float.intBitsToFloat(Buffer.bytesToInt(buffer, offset));
//        offset += 4;
//        return getDouble(val);
        throw new IllegalStateException("Shouldn't be a float");
      }

      case 0xcb: { // double
//        double val = Double.longBitsToDouble(Buffer.bytesToLong(buffer, offset));
//        offset += 8;
//        return getDouble(val);
        throw new IllegalStateException("Shouldn't be a double");
      }

      case 0xd0: { // signed 8 bit integer
        //return (int) buffer[readIdx++];
        throw new IllegalStateException("Shouldn't be an int");
      }

      case 0xcc: { // unsigned 8 bit integer
        //return (int) buffer[readIdx++] & 0xff;
        throw new IllegalStateException("Shouldn't be an int");
      }

      case 0xd1: { // signed 16 bit integer
        short val = Buffer.bigSigned16ToShort(buffer, readIdx);
        readIdx += 2;
        //return (int) val;
        throw new IllegalStateException("Shouldn't be an int");
      }

      case 0xcd: { // unsigned 16 bit integer
        int val = Buffer.bytesToShort(buffer, readIdx);
        readIdx += 2;
        //return (int) val;
        throw new IllegalStateException("Shouldn't be an int");
      }

      case 0xd2: { // signed 32 bit integer
        int val = Buffer.bytesToInt(buffer, readIdx);
        readIdx += 4;
        //return (int) val;
        throw new IllegalStateException("Shouldn't be an int");
      }

      case 0xce: { // unsigned 32 bit integer
        long val = Buffer.bigUnsigned32ToLong(buffer, readIdx);
        readIdx += 4;
        //return (int) val;
        throw new IllegalStateException("Shouldn't be an int");
      }

      case 0xd3: { // signed 64 bit integer
        long val = Buffer.bytesToLong(buffer, readIdx);
        readIdx += 8;
        //return (int) val;
        throw new IllegalStateException("Shouldn't be an int");
      }

      case 0xcf: { // unsigned 64 bit integer
        // Java is constrained to signed longs, so that is the best we can do here.
//        long val = Buffer.bytesToLong(buffer, offset);
//        offset += 8;
//        return getLong(val);
        throw new IllegalStateException("Shouldn't be an unsigned int");
      }

      case 0xc4:
      case 0xd9: { // string/raw bytes with 8 bit header
        int count = buffer[readIdx++] & 0xff;
        return unpackBlob(count);
      }

      case 0xc5:
      case 0xda: { // string/raw bytes with 16 bit header
        int count = Buffer.bytesToShort(buffer, readIdx);
        readIdx += 2;
        return unpackBlob(count);
      }

      case 0xc6:
      case 0xdb: { // string/raw bytes with 32 bit header
        // Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
        int count = Buffer.bytesToInt(buffer, readIdx);
        readIdx += 4;
        return unpackBlob(count);
      }

      case 0xdc: { // list with 16 bit header
//        int count = Buffer.bytesToShort(buffer, offset);
//        offset += 2;
//        return unpackList(count);
        throw new IllegalStateException("Shouldn't be a list");
      }

      case 0xdd: { // list with 32 bit header
        // Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
//        int count = Buffer.bytesToInt(buffer, offset);
//        offset += 4;
//        return unpackList(count);
        throw new IllegalStateException("Shouldn't be a list");
      }

      case 0xde: { // map with 16 bit header
//        int count = Buffer.bytesToShort(buffer, offset);
//        offset += 2;
//        return unpackMap(count);
        throw new IllegalStateException("Shouldn't be a map");
      }

      case 0xdf: { // map with 32 bit header
//        // Java array length is restricted to positive int values (0 - Integer.MAX_VALUE).
//        int count = Buffer.bytesToInt(buffer, offset);
//        offset += 4;
//        return unpackMap(count);
        throw new IllegalStateException("Shouldn't be a map");
      }

      case 0xd4: { // Skip over type extension with 1 byte
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 1;
        return null;
      }

      case 0xd5: { // Skip over type extension with 2 bytes
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 2;
        return null;
      }

      case 0xd6: { // Skip over type extension with 4 bytes
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 4;
        return null;
      }

      case 0xd7: { // Skip over type extension with 8 bytes
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 8;
        return null;
      }

      case 0xd8: { // Skip over type extension with 16 bytes
        //LOGGER.warn("Type extension??");
        readIdx += 1 + 16;
        return null;
      }

      case 0xc7: { // Skip over type extension with 8 bit header and bytes
        //LOGGER.warn("Type extension??");
        int count = buffer[readIdx] & 0xff;
        readIdx += count + 1 + 1;
        return null;
      }

      case 0xc8: { // Skip over type extension with 16 bit header and bytes
        //LOGGER.warn("Type extension??");
        int count = Buffer.bytesToShort(buffer, readIdx);
        readIdx += count + 1 + 2;
        return null;
      }

      case 0xc9: { // Skip over type extension with 32 bit header and bytes
        //LOGGER.warn("Type extension??");
        int count = Buffer.bytesToInt(buffer, readIdx);
        readIdx += count + 1 + 4;
        return null;
      }

      default: {
        if ((type & 0xe0) == 0xa0) { // raw bytes with 8 bit combined header
          return unpackBlob(type & 0x1f);
        }

        if ((type & 0xf0) == 0x80) { // map with 8 bit combined header
          //return unpackMap(type & 0x0f);
          throw new IllegalStateException("Shouldn't be a map");
        }

        if ((type & 0xf0) == 0x90) { // list with 8 bit combined header
          //return unpackList(type & 0x0f);
          throw new IllegalStateException("Shouldn't be a list");
        }

        if (type < 0x80) { // 8 bit combined unsigned integer
          //return type;
          throw new IllegalStateException("Shouldn't be an int");
        }

        if (type >= 0xe0) { // 8 bit combined signed integer
          //return type - 0xe0 - 32;
          throw new IllegalStateException("Shouldn't be an int");
        }
        throw new IllegalStateException("Unknown unpack type: " + type);
      }
    }
  }

  private Boolean unpackBlob(int count) {
    int type = buffer[readIdx++] & 0xff;
    count--;

    switch (type) {
      case ParticleType.STRING:
        //val = getString(Buffer.utf8ToString(buffer, offset, count));
        //break;
        throw new IllegalStateException("Shouldn't be a string");

      case ParticleType.JBLOB:
//        ByteArrayInputStream bastream = new ByteArrayInputStream(buffer, offset, count);
//        ObjectInputStream oistream = new ObjectInputStream(bastream);
//        val = getJavaBlob(oistream.readObject());
//        break;
        throw new IllegalStateException("Shouldn't be a jblob");

      case ParticleType.GEOJSON:
//        val = getGeoJSON(Buffer.utf8ToString(buffer, offset, count));
//        break;
        throw new IllegalStateException("Shouldn't be a geojson");

      default:
        //val = getBlob(Arrays.copyOfRange(buffer, offset, offset + count));
        offsets[keysIdx - 1] = readIdx;
        lengths[keysIdx - 1] = count;
    }
    readIdx += count;
    return true;
  }

  private void sort(final int high, final int low) {
    if (low >= high) {
      return;
    }

    final int partitionIndex = partition(low, high);
    sort(low, partitionIndex - 1);
    sort(partitionIndex + 1, high);
  }

  private int partition(final int low, final int high) {
    int pivot = keys[high];
    int i = low - 1;
    for (int x = low; x < high; x++) {
      if (keys[x] < pivot) {
        i++;

        // swap everything
        int temp = keys[i];
        keys[i] = keys[x];
        keys[x] = temp;
        
        temp = offsets[i];
        offsets[i] = offsets[x];
        offsets[x] = temp;
        
        temp = lengths[i];
        lengths[i] = lengths[x];
        lengths[x] = temp;
      }
    }
    
    i++;
    int temp = keys[i];
    keys[i] = keys[high];
    keys[high] = temp;

    temp = offsets[i];
    offsets[i] = offsets[high];
    offsets[high] = temp;

    temp = lengths[i];
    lengths[i] = lengths[high];
    lengths[high] = temp;
    return i;
  }
}