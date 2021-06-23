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

import gnu.crypto.hash.RipeMD160;

import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * TODO - WAS to be an extension of GNU's implementation but GPL... Try extending
 * Bouncy Castle's or implement our own.
 */
public class UpdatingRipeMD160  {
    public static final int DIGEST_SIZE = 20;

    private RipeMD160 hasher;
    private byte[] lastDigest;

    /**
     * Default ctor just initializes the class.
     */
    public UpdatingRipeMD160() {
        hasher = new RipeMD160();
    }

    public byte[] digest() {
        lastDigest = hasher.digest();
        hasher = new RipeMD160();
        return lastDigest;
    }

    /** @return The previously computed digest.
     * <b>WARNING</b> Make sure to call {@link #digest()} } before this method. */
    public byte[] getDigest() {
        return lastDigest;
    }

    public void update(byte b) {
        hasher.update(b);
    }

    public void update(byte[] b, int offset, int len) {
        hasher.update(b, offset, len);
    }
}
