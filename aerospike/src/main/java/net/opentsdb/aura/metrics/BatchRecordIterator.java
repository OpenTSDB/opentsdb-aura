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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * A buffer for batch reads that is populated only with the data off the socket
 * for the record results. All results are concatenated into a single buffer.
 * Particle type, index and size are stored in arrays and populate the getters
 * when the {@link #advance()} method is called.
 */
public class BatchRecordIterator {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchRecordIterator.class);

  /** Initial size of the main buffer. */
  public static final int INITIAL_BUFFER_LENGTH = 1024 * 1024 * 10;
  public static final int INITIAL_ARRAYS_LENGTH = 1024;

  /** The result code of the batch. */
  protected int resultCode;

  /** The buffer. */
  protected byte[] buffer = new byte[INITIAL_BUFFER_LENGTH];

  /** Write index into the buffer. */
  protected int bufferIndex;

  /** Index of the record amongst the keys. */
  protected int[] keyIdx = new int[1024];

  /** Particle arrays. Type, start index into the buffer and length. */
  protected byte[] particleType = new byte[1024];
  protected int[] particleIndex = new int[1024];
  protected int[] particleLength = new int[1024];
  /** Write index into the particle arrays. */
  protected int writeIndex;

  /** Read index into the particle arrays. */
  protected int readIndex;

  /**
   * Resets the indices
   */
  protected void reset() {
    bufferIndex = 0;
    writeIndex = 0;
    readIndex = 0;
  }

  /**
   * Sets the result code from AS.
   * @param resultCode The result code.
   */
  protected void setResultCode(final int resultCode) {
    this.resultCode = resultCode;
  }

  /**
   * Adds a particle to the buffer.
   * @param keyIdx The index in the incoming key array of this record.
   * @param bis The input stream.
   * @param particleType The type of particle.
   * @param particleLength The length of the particle.
   * @return The number of bytes read (i.e. the length of the particle)
   * @throws IOException If something goes pear shaped.
   */
  protected int read(final int keyIdx,
                     final InputStream bis,
                     final byte particleType,
                     final int particleLength) throws IOException {
    if (bufferIndex + particleLength >= buffer.length) {
      byte[] temp = new byte[bufferIndex + particleLength];
      System.arraycopy(buffer, 0, temp, 0, bufferIndex);
      buffer = temp;
    }

    int pos = 0;
    int startIdx = bufferIndex;
    while (pos < particleLength) {
      int count = bis.read(buffer, bufferIndex, particleLength - pos);

      if (count < 0) {
        throw new EOFException();
      }
      pos += count;
      bufferIndex += count;
    }

    if (writeIndex + 1 >= this.particleType.length) {
      byte[] tempTypes = new byte[this.particleType.length * 2];
      System.arraycopy(this.particleType, 0, tempTypes, 0, writeIndex);
      this.particleType = tempTypes;

      int[] temp = new int[this.keyIdx.length * 2];
      System.arraycopy(this.keyIdx, 0, temp, 0, writeIndex);
      this.keyIdx = temp;

      temp = new int[this.particleType.length * 2];
      System.arraycopy(particleIndex, 0, temp, 0, writeIndex);
      particleIndex = temp;

      temp = new int[this.particleLength.length * 2];
      System.arraycopy(this.particleLength, 0, temp, 0, writeIndex);
      this.particleLength = temp;
    }

    this.keyIdx[writeIndex] = keyIdx;
    this.particleType[writeIndex] = particleType;
    particleIndex[writeIndex] = startIdx;
    this.particleLength[writeIndex] = particleLength;
    writeIndex++;

    return particleLength;
  }

  /**
   * Method to sort the results on the key indices in ascending order.
   */
  public void sort() {
    if (writeIndex < 2) {
      return;
    }
    sort(0, writeIndex - 1);
  }

  /**
   * Instead of creating iterator objects, this will reset the read index so
   * we can iterate multiple times if needed (don't do that!!!)
   */
  public void resetIterator() {
    readIndex = 0;
  }

  /**
   * @return The AS result code for the batch.
   */
  public int getResultCode() {
    return resultCode;
  }

  /**
   * @return Whether or not there is another record to read.
   */
  public boolean hasNext() {
    return readIndex < writeIndex;
  }

  /**
   * @return True if a new record was loaded and read to parse.
   */
  public boolean advance() {
    if (readIndex + 1 < writeIndex) {
      readIndex++;
      return true;
    }
    // shouldn't happen
    return false;
  }

  /**
   * @return The index of the key for this record in the request key array.
   */
  public int keyIndex() {
    return keyIdx[readIndex];
  }

  /**
   * @return The buffer.
   */
  public byte[] buffer() {
    return buffer;
  }

  /**
   * @return The starting index of the particle payload in the {@link #buffer()}.
   */
  public int particleIndex() {
    return particleIndex[readIndex];
  }

  /**
   * @param keyIndex The index into the records array.
   * @return The particle start index into the buffer.
   */
  public int particleIndex(final int keyIndex) {
    return particleIndex[keyIndex];
  }

  /**
   * @return The length of the particle payload.
   */
  public int particleLength() {
    return particleLength[readIndex];
  }

  /**
   * @param keyIndex The index into the records array.
   * @return The particle length.
   */
  public int particleLength(final int keyIndex) {
    return particleLength[keyIndex];
  }

  /**
   * @return The type of particle.
   */
  public byte particleType() {
    return particleType[readIndex];
  }

  /**
   * @param keyIndex The index into the records array.
   * @return The particle type.
   */
  public byte particleType(final int keyIndex) {
    return particleType[keyIndex];
  }

  /**
   * @return The particle as a Java object. Don't use this as it's inefficient!
   */
  public Object particleToObject() {
    return Buffer.bytesToParticle(particleType[readIndex],
            buffer,
            particleIndex[readIndex],
            particleLength[readIndex]);
  }

  /**
   * @return The number of records found.
   */
  public int entries() {
    return writeIndex;
  }

  /**
   * @return The array if indices into the original key array.
   * <b>WARNING:</b> Do NOT modify this array or the entire payload will be
   * invalidated.
   */
  public int[] keyIndices() {
    return keyIdx;
  }

  private void sort(final int low, final int high) {
    if (low >= high) {
      return;
    }

    final int partitionIndex = partition(low, high);
    sort(low, partitionIndex - 1);
    sort(partitionIndex + 1, high);
  }

  private int partition(final int low, final int high) {
    int pivot = keyIdx[high];
    int i = low - 1;
    for (int x = low; x < high; x++) {
      if (keyIdx[x] < pivot) {
        i++;
        
        // sawp everything
        int temp = keyIdx[i];
        keyIdx[i] = keyIdx[x];
        keyIdx[x] = temp;
        
        byte btemp = particleType[i];
        particleType[i] = particleType[x];
        particleType[x] = btemp;
        
        temp = particleIndex[i];
        particleIndex[i] = particleIndex[x];
        particleIndex[x] = temp;
        
        temp = particleLength[i];
        particleLength[i] = particleLength[x];
        particleLength[x] = temp;
      }
    }

    // swap arr[i+1] and arr[high] (or pivot)
    i++;
    int temp = keyIdx[i];
    keyIdx[i] = keyIdx[high];
    keyIdx[high] = temp;

    byte btemp = particleType[i];
    particleType[i] = particleType[high];
    particleType[high] = btemp;

    temp = particleIndex[i];
    particleIndex[i] = particleIndex[high];
    particleIndex[high] = temp;

    temp = particleLength[i];
    particleLength[i] = particleLength[high];
    particleLength[high] = temp;

    return i;
  }
}
