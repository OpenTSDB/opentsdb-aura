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

import com.aerospike.client.Operation.Type;
import com.aerospike.client.Value;
import com.aerospike.client.util.Packer;
import net.opentsdb.aura.metrics.ByteValue;
import net.opentsdb.aura.metrics.IntValue;

/**
 * Operation that handles serdes on map operations. This is some copy pasta from the
 * AS client with some optimizations to avoid creating objects.
 *
 * TODO - The Packer objects are still created but use a thread local buffer
 * so that's reused at least.
 */
public class MapOperation {
    private static final int SET_TYPE = 64;
    protected static final int ADD = 65;
    protected static final int ADD_ITEMS = 66;
    protected static final int PUT = 67;
    protected static final int PUT_ITEMS = 68;
    protected static final int REPLACE = 69;
    protected static final int REPLACE_ITEMS = 70;
    private static final int INCREMENT = 73;
    private static final int DECREMENT = 74;
    private static final int CLEAR = 75;
    private static final int REMOVE_BY_KEY = 76;
    private static final int REMOVE_BY_INDEX = 77;
    private static final int REMOVE_BY_RANK = 79;
    private static final int REMOVE_BY_KEY_LIST = 81;
    private static final int REMOVE_BY_VALUE = 82;
    private static final int REMOVE_BY_VALUE_LIST = 83;
    private static final int REMOVE_BY_KEY_INTERVAL = 84;
    private static final int REMOVE_BY_INDEX_RANGE = 85;
    private static final int REMOVE_BY_VALUE_INTERVAL = 86;
    private static final int REMOVE_BY_RANK_RANGE = 87;
    private static final int REMOVE_BY_KEY_REL_INDEX_RANGE = 88;
    private static final int REMOVE_BY_VALUE_REL_RANK_RANGE = 89;
    private static final int SIZE = 96;
    private static final int GET_BY_KEY = 97;
    private static final int GET_BY_INDEX = 98;
    private static final int GET_BY_RANK = 100;
    private static final int GET_BY_VALUE = 102;  // GET_ALL_BY_VALUE on server.
    private static final int GET_BY_KEY_INTERVAL = 103;
    private static final int GET_BY_INDEX_RANGE = 104;
    private static final int GET_BY_VALUE_INTERVAL = 105;
    private static final int GET_BY_RANK_RANGE = 106;
    private static final int GET_BY_KEY_LIST = 107;
    private static final int GET_BY_VALUE_LIST = 108;
    private static final int GET_BY_KEY_REL_INDEX_RANGE = 109;
    private static final int GET_BY_VALUE_REL_RANK_RANGE = 110;

    private final OperationLocal operation;
    /** When int keys are used we'll re-use this object to pack the value. */
    private final IntValue intKey = new IntValue(0);

    /** The packer payload once it's populated. */
    private final ByteValue valueBytes = new ByteValue(null);

    /**
     * Default ctor.
     */
    public MapOperation() {

        this.operation = new OperationLocal(Type.MAP_MODIFY, null, valueBytes);
    }

    /**
     * Allows fetching map entries by a range. At least one of keyBegin or KeyEnd must
     * be present.
     * @param binName The optional bin name.
     * @param keyBegin The optional key beginning, inclusive.
     * @param keyEnd The optional key end, exclusive.
     * @param returnType The response we want.
     * @return The operation.
     */
    public OperationLocal getByKeyRange(final byte[] binName,
                                        Value keyBegin,
                                        final Value keyEnd,
                                        final int returnType) {
        // GET_BY_KEY_INTERVAL == 103
        // return type == 7 for our case
        Packer packer = new Packer();
        packer.packRawShort(GET_BY_KEY_INTERVAL);

        if (keyBegin == null) {
            keyBegin = Value.getAsNull();
        }

        if (keyEnd == null) {
            packer.packArrayBegin(2);
            packer.packInt(returnType);
            keyEnd.pack(packer);
        }
        else {
            packer.packArrayBegin(3);
            packer.packInt(returnType);
            keyBegin.pack(packer);
            keyEnd.pack(packer);
        }
        valueBytes.bytes = packer.toByteArray();
        operation.type = Type.MAP_READ;
        operation.binName = binName;

        return operation;
    }

    /**
     *
     Allows fetching map entries by a range.
     * @param binName The optional bin name.
     * @param keyBegin The optional key beginning, inclusive.
     * @param keyEnd The optional key end, exclusive.
     * @param returnType The response we want.
     * @return The operation.
     */
    public OperationLocal getByKeyRange(final byte[] binName,
                                        final int keyBegin,
                                        final int keyEnd,
                                        final int returnType) {
        // GET_BY_KEY_INTERVAL == 103
        Packer packer = new Packer();
        packer.packRawShort(GET_BY_KEY_INTERVAL);

        packer.packArrayBegin(3);
        packer.packInt(returnType);
        intKey.value = keyBegin;
        intKey.pack(packer);
        intKey.value = keyEnd;
        intKey.pack(packer);

        valueBytes.bytes = packer.toByteArray();
        operation.type = Type.MAP_READ;
        operation.binName = binName;

        return operation;
    }

    /**
     * Stores a value in the map.
     * @param policy The non-null policy.
     * @param binName An optional bin name.
     * @param key The non-null key.
     * @param value The value to store.
     * @return The operation.
     */
    public OperationLocal put(final MapPolicy policy,
                              final byte[] binName,
                              final Value key,
                              final Value value) {
        //Will need to be optimized further
        Packer packer = new Packer();

        if (policy.flags != 0) {
            packer.packRawShort(PUT);
            packer.packArrayBegin(4);
            key.pack(packer);
            value.pack(packer);
            packer.packInt(policy.attributes);
            packer.packInt(policy.flags);
        }
        else {
            packer.packRawShort(policy.itemCommand);

            if (policy.itemCommand == REPLACE) {
                // Replace doesn't allow map attributes because it does not create on non-existing key.
                packer.packArrayBegin(2);
                key.pack(packer);
                value.pack(packer);
            }
            else {
                packer.packArrayBegin(3);
                key.pack(packer);
                value.pack(packer);
                packer.packInt(policy.attributes);
            }
        }
        valueBytes.bytes = packer.toByteArray();
        operation.type = Type.MAP_MODIFY;
        operation.binName = binName;

        return operation;
    }

    /**
     Stores a value in the map.
     * @param policy The non-null policy.
     * @param binName An optional bin name.
     * @param key The non-null key.
     * @param value The value to store.
     * @return The operation.
     */
    public OperationLocal put(final MapPolicy policy,
                              final byte[] binName,
                              final int key,
                              final Value value) {
        //Will need to be optimized further
        Packer packer = new Packer();
        intKey.value = key;
        if (policy.flags != 0) {
            packer.packRawShort(PUT);
            packer.packArrayBegin(4);
            intKey.pack(packer);
            value.pack(packer);
            packer.packInt(policy.attributes);
            packer.packInt(policy.flags);
        }
        else {
            packer.packRawShort(policy.itemCommand);

            if (policy.itemCommand == REPLACE) {
                // Replace doesn't allow map attributes because it does not create on non-existing key.
                packer.packArrayBegin(2);
                intKey.pack(packer);
                value.pack(packer);
            }
            else {
                packer.packArrayBegin(3);
                intKey.pack(packer);
                value.pack(packer);
                packer.packInt(policy.attributes);
            }
        }
        valueBytes.bytes = packer.toByteArray();
        operation.type = Type.MAP_MODIFY;
        operation.binName = binName;

        return operation;
    }

    /**
     Stores a value in the map.
     * @param policy The non-null policy.
     * @param binName An optional bin name.
     * @param key The non-null key.
     * @param value The value to store.
     * @return The operation.
     */
    public OperationLocal put(final MapPolicy policy,
                              final byte[] binName,
                              final int key,
                              final byte[] value) {
        //Will need to be optimized further
        Packer packer = new Packer();
        intKey.value = key;
        valueBytes.bytes = value;
        if (policy.flags != 0) {
            packer.packRawShort(PUT);
            packer.packArrayBegin(4);
            intKey.pack(packer);
            valueBytes.pack(packer);
            packer.packInt(policy.attributes);
            packer.packInt(policy.flags);
        }
        else {
            packer.packRawShort(policy.itemCommand);

            if (policy.itemCommand == REPLACE) {
                // Replace doesn't allow map attributes because it does not create on non-existing key.
                packer.packArrayBegin(2);
                intKey.pack(packer);
                valueBytes.pack(packer);
            }
            else {
                packer.packArrayBegin(3);
                intKey.pack(packer);
                valueBytes.pack(packer);
                packer.packInt(policy.attributes);
            }
        }
        valueBytes.bytes = packer.toByteArray();
        operation.type = Type.MAP_MODIFY;
        operation.binName = binName;

        return operation;
    }
}
