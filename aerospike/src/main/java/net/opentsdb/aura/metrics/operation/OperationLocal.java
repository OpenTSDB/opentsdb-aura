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

package net.opentsdb.aura.metrics.operation;

import com.aerospike.client.Bin;
import com.aerospike.client.Operation.Type;
import com.aerospike.client.Value;

import java.nio.charset.StandardCharsets;

public final class OperationLocal {
    /**
     * Create read bin database operation.
     * @param binName The bin name.
     * @return The operation.
     */
    public static OperationLocal get(byte[] binName) {
        return new OperationLocal(Type.READ, binName);
    }

    /**
     * @return Create read all record bins database operation.
     */
    public static OperationLocal get() {
        return new OperationLocal(Type.READ);
    }

    /**
     * @return Create read record header database operation.
     */
    public static OperationLocal getHeader() {
        return new OperationLocal(Type.READ_HEADER);
    }

    /**
     * Create set database operation.
     * @param bin The bin
     * @return The operation.
     */
    public static OperationLocal put(Bin bin) {
        return new OperationLocal(Type.WRITE, bin.name.getBytes(StandardCharsets.UTF_8), bin.value);
    }

    /**
     * Create string append database operation.
     * @param bin The bin
     * @return The operation.
     */
    public static OperationLocal append(Bin bin) {
        return new OperationLocal(Type.APPEND, bin.name.getBytes(StandardCharsets.UTF_8), bin.value);
    }

    /**
     * Create string prepend database operation.
     * @param bin The bin
     * @return The operation.
     */
    public static OperationLocal prepend(Bin bin) {
        return new OperationLocal(Type.PREPEND, bin.name.getBytes(StandardCharsets.UTF_8), bin.value);
    }

    /**
     * Create integer add database operation.
     * @param bin The bin
     * @return The operation.
     */
    public static OperationLocal add(Bin bin) {
        return new OperationLocal(Type.ADD, bin.name.getBytes(StandardCharsets.UTF_8), bin.value);
    }

    /**
     * @return Create touch database operation.
     */
    public static OperationLocal touch() {
        return new OperationLocal(Type.TOUCH);
    }
    

    /**
     * Type of operation.
     */
    public Type type;

    /**
     * Optional bin name used in operation.
     */
    public byte[] binName;
    public byte[][] binNames;

    /**
     * Optional argument to operation.
     */
    public Value value;

    public OperationLocal(Type type, byte[] binName, Value value) {
        this.type = type;
        this.binName = binName;
        this.value = value;
    }

    public OperationLocal(Type type, byte[] binName) {
        this.type = type;
        this.binName = binName;
        this.value = Value.getAsNull();
    }

    public OperationLocal(Type type, byte[][] binNames) {
        this.type = type;
        this.binNames = binNames;
        this.value = Value.getAsNull();
    }

    private OperationLocal(Type type) {
        this.type = type;
        this.binName = null;
        this.value = Value.getAsNull();
    }
}
