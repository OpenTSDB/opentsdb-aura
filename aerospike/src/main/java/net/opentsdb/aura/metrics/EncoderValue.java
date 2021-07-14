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
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.util.Packer;
import net.opentsdb.aura.metrics.core.TimeSeriesEncoder;
import org.luaj.vm2.LuaValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class EncoderValue extends Value {
    private static final Logger LOGGER = LoggerFactory.getLogger(EncoderValue.class);

    private TimeSeriesEncoder encoder;
    int serializationLength;
    // TODO - ugly temp hack!!!
    Field buffer;
    Field offset;
    Method packByteArrayBegin;
    Method resize;

    public EncoderValue() {
        try {
            buffer = Packer.class.getDeclaredField("buffer");
            buffer.setAccessible(true);
            offset = Packer.class.getDeclaredField("offset");
            offset.setAccessible(true);
            packByteArrayBegin = Packer.class.getDeclaredMethod("packByteArrayBegin", int.class);
            packByteArrayBegin.setAccessible(true);
            resize = Packer.class.getDeclaredMethod("resize", int.class);
            resize.setAccessible(true);
        } catch (NoSuchFieldException | NoSuchMethodException e) {
            throw new RuntimeException("Couldn't extract fields or methods", e);
        }
    }

    public void setEncoder(final TimeSeriesEncoder encoder) {
        this.encoder = encoder;
        serializationLength = encoder.serializationLength();
    }

    @Override
    public int estimateSize() throws AerospikeException {
        return serializationLength;
    }

    @Override
    public int write(byte[] buffer, int offset) throws AerospikeException {
        encoder.serialize(buffer, offset, serializationLength);
        return serializationLength;
    }

    @Override
    public void pack(Packer packer) {
        try {
            packByteArrayBegin.invoke(packer, serializationLength + 1);
            packer.packByte(ParticleType.BLOB);

            byte[] buf = (byte[]) buffer.get(packer);
            int of = (int) offset.get(packer);
            if (of + serializationLength > buf.length) {
                resize.invoke(serializationLength);
                buf = (byte[]) buffer.get(packer);
            }
            encoder.serialize(buf, of, serializationLength);
            of += serializationLength;
            offset.set(packer, of);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.error("Failed to write to AS buffer", e);
        }
    }

    @Override
    public int getType() {
        return ParticleType.BLOB;
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
