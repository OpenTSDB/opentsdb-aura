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

package net.opentsdb.aura.metrics.core.metaflush;

import net.opentsdb.aura.metrics.metaflush.CompressedMetaWriter;
import net.opentsdb.aura.metrics.metaflush.Uploader;
import net.opentsdb.aura.metrics.core.data.ByteArrays;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Go to the the read tag/read metric methods only after read len.
 */
public class TestUploader implements Uploader {

    private final int shardId;
    private int timestamp;
    private byte[] uncompressed;
    private GZIPInputStream gzipInputStream;
    private int currentPosition;
    private int nextRecordStart = 0;

    public TestUploader(int shardId) {

        this.shardId = shardId;
    }

    @Override
    public void upload(int timestamp, byte[] payload) {
        this.timestamp = timestamp;
        try {
            gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(payload));
            uncompressed = new byte[4096];
            byte[] buf = new byte[4096];
            int read = 0;
            int stored = 0;
            while ((read = gzipInputStream.read(buf)) > 0) {
                if (read + stored >= uncompressed.length) {
                    uncompressed = Arrays.copyOf(uncompressed, uncompressed.length + Math.max(read, 1024));
                }
                System.arraycopy(buf, 0, uncompressed, stored, read);
                stored += read;
            }
            uncompressed = Arrays.copyOf(uncompressed, stored);
            // 9+
            //this.uncompressed = gzipInputStream.readAllBytes();
        } catch (IOException e) {
            throw new AssertionError("Error reading from gzip stream", e);
        }
    }

    public int readLen() {
        final int i = ByteArrays.getInt(uncompressed, nextRecordStart);
        this.nextRecordStart = this.currentPosition + i + 4;
        this.currentPosition += 4;
        return i;
    }

    public void readRecordHeader() {
        advance(CompressedMetaWriter.Type.SEPARATOR);
        advance(CompressedMetaWriter.Type.RECORD);
    }


    public String readMetric(Charset charset) {
        return read(CompressedMetaWriter.Type.METRIC, charset);
    }

    public String readTag(Charset charset) {
        return read(CompressedMetaWriter.Type.TAG, charset);
    }

    private String read(CompressedMetaWriter.Type type, Charset charset) {
        advance(CompressedMetaWriter.Type.SEPARATOR);
        advance(type);
        byte[] buf = readSection();
        return new String(buf, charset);
    }

    private byte[] readSection() {

        final int length = getSectionLength();
        byte[] buffer = new byte[length];
        System.arraycopy(uncompressed, currentPosition, buffer, 0, length);
        currentPosition += length;
        return buffer;
    }

    private int getSectionLength() {
        int pos = currentPosition;
        while (pos < this.nextRecordStart && (uncompressed[pos] != CompressedMetaWriter.Type.SEPARATOR.get())) {
            pos++;
        }
        return (pos - currentPosition);

    }

    public long readHash() {
        advance(CompressedMetaWriter.Type.SEPARATOR);
        advance(CompressedMetaWriter.Type.HASH);
        final long hash = ByteArrays.getLong(uncompressed, currentPosition);
        currentPosition += 8;
        return hash;
    }

    private void advance(CompressedMetaWriter.Type type) {
        assertTrue(uncompressed[currentPosition++] == type.get());
    }

}
