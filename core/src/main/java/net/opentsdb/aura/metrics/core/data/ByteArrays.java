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

/**
 * TODO: move to common library. This code is needed in multiple projects.
 */
public interface ByteArrays {

    /**
     * @param offset offset in the buffer, inclusive
     */
    static void putLong(final long l, final byte[] buf, final int offset) {
        buf[offset] = (byte) (l >> 56);
        buf[offset + 1] = (byte) (l >> 48);
        buf[offset + 2] = (byte) (l >> 40);
        buf[offset + 3] = (byte) (l >> 32);
        buf[offset + 4] = (byte) (l >> 24);
        buf[offset + 5] = (byte) (l >> 16);
        buf[offset + 6] = (byte) (l >> 8);
        buf[offset + 7] = (byte) (l);
    }

    static byte[] longToArray(final long l) {
        byte[] buf = new byte[8];
        putLong(l, buf, 0);
        return buf;
    }

    static void longToArray(final long l, final byte[] buf) {
        putLong(l, buf, 0);
    }

    /**
     * @param offset offset from to read a long, inclusive
     */
    static long getLong(final byte[] buf, final int offset) {
        return ((((long) buf[offset]) << 56) |
            (((long) buf[offset + 1] & 0xff) << 48) |
            (((long) buf[offset + 2] & 0xff) << 40) |
            (((long) buf[offset + 3] & 0xff) << 32) |
            (((long) buf[offset + 4] & 0xff) << 24) |
            (((long) buf[offset + 5] & 0xff) << 16) |
            (((long) buf[offset + 6] & 0xff) << 8) |
            (((long) buf[offset + 7] & 0xff)));
    }

    /**
     * @param offset offset in the buffer, inclusive
     */
    static void putInt(final int i, final byte[] buf, final int offset) {
        buf[offset] = (byte) (i >> 24);
        buf[offset + 1] = (byte) (i >> 16);
        buf[offset + 2] = (byte) (i >> 8);
        buf[offset + 3] = (byte) (i);
    }

    static byte[] intToArray(final int i) {
        byte[] buf = new byte[4];
        putInt(i, buf, 0);
        return buf;
    }

    /**
     * @param offset offset from to read a long, inclusive
     */
    static int getInt(final byte[] buf, final int offset) {
        return (((int) buf[offset] & 0xff) << 24) |
            (((int) buf[offset + 1] & 0xff) << 16) |
            (((int) buf[offset + 2] & 0xff) << 8) |
            (((int) buf[offset + 3] & 0xff));
    }

    static short getShort(final byte[] buf, final int offset) {
        return (short) ((buf[offset] << 8) | (buf[offset + 1] & 0xff));
    }

    /**
     * @param offset offset in the buffer, inclusive
     */
    static void putShort(final short s, final byte[] buf, final int offset) {
        buf[offset] = (byte) (s >> 8);
        buf[offset + 1] = (byte) (s);
    }


    static float getFloat(final byte[] buf, final int offset) {
        return Float.intBitsToFloat(getInt(buf, offset));
    }

    static void putFloat(final float f, final byte[] buf, final int offset) {
        putInt(Float.floatToRawIntBits(f), buf, offset);
    }

}
