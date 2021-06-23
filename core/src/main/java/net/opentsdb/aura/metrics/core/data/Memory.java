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

package net.opentsdb.aura.metrics.core.data;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Don't shoot yourself in leg.
 *
 * Not aligned.
 */
public class Memory {
    public static final Unsafe unsafe;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (NoSuchFieldException|IllegalAccessException  e) {
            throw new RuntimeException("Failed to get unsafe instance, are you running Oracle JDK?", e);
        }
    }

    /**
     * WARNING: will not clear the memory - i.e. it can contain garbage
     * @return address
     */
    public static long malloc(long size) {
        return unsafe.allocateMemory(size);
    }

    /**
     *
     * @param addr address to write
     * @param data data buffer
     * @param length bytes to write. If more than allocated, bad.
     */
    public static void write(long addr, byte[] data, long length) {
        unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, addr, length);
    }

    public static void write(long addr, byte[] data, int offset, long length) {
        unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, null, addr, length);
    }

    public static void read(long addr, byte[] buf, long length) {
        unsafe.copyMemory(null, addr, buf, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
    }

    public static void read(long addr, byte[] buf, int offset, long length) {
        unsafe.copyMemory(null, addr, buf, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, length);
    }

    public static void free(long addr) {
        unsafe.freeMemory(addr);
    }

    public static void write(long addr, long[] data, long length) {
        unsafe.copyMemory(data, Unsafe.ARRAY_LONG_BASE_OFFSET, null, addr, length);
    }

    public static void read(long addr, long[] buf, long length) {
        unsafe.copyMemory(null, addr, buf, Unsafe.ARRAY_LONG_BASE_OFFSET, length);
    }


    public static int getInt(long addr) {
        return unsafe.getInt(addr);
    }

    public static byte getByte(long addr) {
        return unsafe.getByte(addr);
    }

    public static short getShort(long addr) {
        return unsafe.getShort(addr);
    }

    public static short getShortBigEndian(long addr) {
        return Short.reverseBytes(unsafe.getShort(addr));
    }

    public static int getIntBigEndian(long addr) {
        return Integer.reverseBytes(unsafe.getInt(addr));
    }

    public static long getLong(long addr) {
      return unsafe.getLong(addr);
    }

    public static long getLongBigEndian(long addr) {
      return Long.reverseBytes(unsafe.getLong(addr));
    }
}
