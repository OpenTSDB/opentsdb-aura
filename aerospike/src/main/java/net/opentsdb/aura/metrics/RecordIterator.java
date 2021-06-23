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

import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Buffer;

/**
 * The default Aerospike record parses all of the records into their unpacked
 * objects. This can get nasty for complex records (e.g. maps) so we're going to
 * hold the buffer and iterate over the results as the caller needs them.
 * <p>
 * Work with this result by first calling {@link #getResultCode()} to make sure there
 * wasn't an error on read. Then call {@link #advance()} until it returns false. Each
 * time it returns true, call the getters to access the current state.
 *
 * TODO - For now we'll still unpack but we can bring up some of that unpacking code
 * into helpers. E.g. byte arrays could be read straight out of the buffer without
 * having to copy into a stand-alone array.
 */
public class RecordIterator {
    /** The {@link ResultCode}. */
    private int resultCode;
    private int opCount;
    private int fieldCount;
    private int generation;
    private int expiration;
    private int receiveOffset;
    private int opIdx;

    /** A ref to the thread-local data buffer of the command this came from. */
    private byte[] dataBuffer;

    /** The type of data at the current index. */
    private byte particleType;
    private int binOffset;
    private byte binSize;
    private int particleOffset;
    private int particleBytesSize;

    /**
     * Package private to reset the entry on an error or OK without data.
     * @param resultCode The result code to set.
     */
    void reset(final int resultCode) {
        this.resultCode = resultCode;
        dataBuffer = null;
        opIdx = opCount = 0;
    }

    /**
     * Package private to parse a successful call. It may still be empty.
     * @param opCount The number of operations found (e.g. items in a map)
     * @param fieldCount The number of fields. The source just skipped them.
     * @param generation The record generation.
     * @param expiration The record expiration.
     * @param dataBuffer A ref to the original buffer.
     */
    void parseRecord(int opCount,
                     int fieldCount,
                     int generation,
                     int expiration,
                     byte[] dataBuffer) {
        resultCode = ResultCode.OK;
        this.opCount = opCount;
        this.fieldCount = fieldCount;
        this.generation = generation;
        this.expiration = expiration;
        this.dataBuffer = dataBuffer;
        receiveOffset = 0;
        opIdx = 0;

        // There can be fields in the response (setname etc).
        // But for now, ignore them. Expose them to the API if needed in the future.
        if (fieldCount > 0) {
            // Just skip over all the fields
            for (int i = 0; i < fieldCount; i++) {
                int fieldSize = Buffer.bytesToInt(dataBuffer, receiveOffset);
                receiveOffset += 4 + fieldSize;
            }
        }
    }

    /** @return The {@link ResultCode}. */
    public int getResultCode() {
        return resultCode;
    }

    /** @return The generation integer. */
    public int getGeneration() {
        return generation;
    }

    /** @return The expiration time for the record. TODO units? */
    public int getExpiration() {
        return expiration;
    }

    /** @return The raw underlying buffer. BE CAREFUL! */
    public byte[] buffer() {
        return dataBuffer;
    }

    /** @return The {@link com.aerospike.client.command.ParticleType} of object stored. */
    public byte type() {
        return particleType;
    }

    /** @return A helper to automatically parse the current object to it's type. This
     * will create some garbage. */
    public Object valueToObject() {
        return Buffer.bytesToParticle(particleType, dataBuffer, particleOffset, particleBytesSize);
    }

    /** @return The bin name converted to a string. */
    public String binName() {
        return Buffer.utf8ToString(dataBuffer, binOffset, binSize);
    }

    /** @return The offset where the object starts on the data buffer. */
    public int getParticleOffset() {
        return particleOffset;
    }

    /** @return The size of the particle in bytes. */
    public int getParticleBytesSize() {
        return particleBytesSize;
    }

    /** @return The offset of the bin in the data buffer. */
    public int getBinOffset() {
        return binOffset;
    }

    /** @return The size of the bin name. */
    public int getBinSize() {
        return binSize;
    }

    /**
     * Call to iterate to the next value.
     * @return True if there is a value to read, false if we have run out of values.
     */
    public boolean advance() {
        if (opIdx >= opCount) {
            return false;
        }

        int opSize = Buffer.bytesToInt(dataBuffer, receiveOffset);
        particleType = dataBuffer[receiveOffset+5];
        binSize = dataBuffer[receiveOffset+7];
        binOffset = receiveOffset + 8;
        receiveOffset += 4 + 4 + binSize;

        particleBytesSize = (int) (opSize - (4 + binSize));
        particleOffset = receiveOffset;
        receiveOffset += particleBytesSize;

        // got something
        opIdx++;
        return true;
    }

}
