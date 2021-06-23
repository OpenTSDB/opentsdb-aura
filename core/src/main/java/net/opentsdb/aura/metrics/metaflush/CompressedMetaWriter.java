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

package net.opentsdb.aura.metrics.metaflush;

import net.opentsdb.aura.metrics.core.data.ByteArrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class CompressedMetaWriter implements MetaWriter {

    private final String namespace;
    private final Uploader uploader;
    private final int shardid;
    private GZIPOutputStream outputStream;
    private ByteArrayOutputStream byteArrayOutputStream;
    private byte[] localBuffer = new byte[1024];
    private byte[] longBuffer = new byte[8];
    private int currentPosition = 0;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private int timestamp;
    public enum Type {
        SEPARATOR((byte)1),
        RECORD((byte)2),
        HASH((byte)3),
        TAG((byte)4),
        METRIC((byte)5);
        byte type;
        Type(byte type) {
            this.type = type;
        }
        public byte get() {
            return type;
        }
    }

    public CompressedMetaWriter(String namespace, Uploader uploader) {
        this(namespace, uploader, 0);
    }

    public CompressedMetaWriter(String namespace, Uploader uploader , int shardid) {
        this.namespace = namespace;
        this.uploader = uploader;
        this.shardid = shardid;
    }

    @Override
    public void init(int timestamp) {
        this.timestamp = timestamp;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            //TODO: Make this pluggable
            outputStream = new GZIPOutputStream(byteArrayOutputStream);
        } catch (IOException e) {
            log.error("Error in meta flush, while creating a Gzip stream", e);
            throw new RuntimeException(e);
        }
        this.currentPosition = 0;
    }

    @Override
    public void writeHash(long hash) {
        ByteArrays.putLong(hash, longBuffer, 0);
        write(longBuffer, 0, 8, Type.HASH);
    }

    private void copyIntoLocalBuffer(byte[] record, int off, int len) {
        System.arraycopy(record, off, localBuffer, currentPosition, len);
        currentPosition += len;
    }

    private void require(int length) {
        int rem = (localBuffer.length - 1) - currentPosition;
        if(length > rem) {

            byte[] byteArray = new byte[(localBuffer.length - rem + length)  * 2];

            log.info("length: {} buf len: {} rem:  {} new buf len: {}", length, localBuffer.length, rem, byteArray.length );

            System.arraycopy(this.localBuffer,0, byteArray, 0, currentPosition);
            this.localBuffer = byteArray;
        }
    }

    @Override
    public void writeTag(byte[] tag, int off, int len) {
        write(tag, off, len, Type.TAG);
    }

    @Override
    public void writeMetric(byte[] metric, int off, int len) {
        write(metric, off, len, Type.METRIC);
    }

    private void write(byte[] input, int off, int len, Type type) {
        require(len + 2);
        localBuffer[currentPosition++] = Type.SEPARATOR.get();
        localBuffer[currentPosition++] = type.get();
        copyIntoLocalBuffer(input, off, len);
    }

    @Override
    public void advanceRecord() {

        if(currentPosition != 0) {
            try {
                ByteArrays.putInt(currentPosition - 4, localBuffer, 0);
                //Write Length
                outputStream.write(localBuffer, 0, currentPosition);
            } catch (IOException e) {
                log.error("Error in meta flush, while writing to a Gzip stream", e);
                throw new RuntimeException(e);
            }
            currentPosition = 0;
        }
        currentPosition += 4;
        localBuffer[currentPosition++] = Type.SEPARATOR.get();
        localBuffer[currentPosition++] = Type.RECORD.get();

    }

    @Override
    public void close() {
        try {
            outputStream.finish();
            outputStream.flush();
            outputStream.close();
            uploader.upload(timestamp, byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            log.error("Error in meta flush, while closing a Gzip stream and upload", e);
        }
    }
}
