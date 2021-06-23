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
import com.aerospike.client.lua.LuaBytes;
import com.aerospike.client.lua.LuaInstance;
import com.aerospike.client.util.Packer;
import org.luaj.vm2.LuaValue;

import java.util.Arrays;

public final class ByteValue extends Value {
    public byte[] bytes;

    public ByteValue(byte[] bytes) {
        this.bytes = bytes;
    }

    public int estimateSize() {
        return this.bytes.length;
    }

    public int write(byte[] buffer, int offset) {
        System.arraycopy(this.bytes, 0, buffer, offset, this.bytes.length);
        return this.bytes.length;
    }

    public void pack(Packer packer) {
        packer.packBytes(this.bytes);
    }

    public int getType() {
        return 4;
    }

    public Object getObject() {
        return this.bytes;
    }

    public LuaValue getLuaValue(LuaInstance instance) {
        return new LuaBytes(instance, this.bytes);
    }

    public String toString() {
        return Buffer.bytesToHexString(this.bytes);
    }

    public boolean equals(Object other) {
        return other != null && this.getClass().equals(other.getClass()) &&
                Arrays.equals(this.bytes, ((net.opentsdb.aura.metrics.ByteValue) other).bytes);
    }

    public int hashCode() {
        return Arrays.hashCode(this.bytes);
    }
}