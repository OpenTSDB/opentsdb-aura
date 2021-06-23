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

import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.util.Packer;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaValue;

public final class IntValue extends Value {
    public int value;

    public IntValue(int value) {
        this.value = value;
    }

    @Override
    public int estimateSize() {
        return 4;
    }

    @Override
    public int write(byte[] buffer, int offset) {
        Buffer.longToBytes(value, buffer, offset);
        return 8;
    }

    @Override
    public void pack(Packer packer) {
        packer.packInt(value);
    }

    @Override
    public int getType() {
        return ParticleType.INTEGER;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public LuaValue getLuaValue(LuaInstance instance) {
        return LuaInteger.valueOf(value);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public boolean equals(Object other) {
        return (other != null &&
                this.getClass().equals(other.getClass()) &&
                this.value == ((IntValue)other).value);
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public int toInteger() {
        return value;
    }

    @Override
    public long toLong() {
        return value;
    }
}