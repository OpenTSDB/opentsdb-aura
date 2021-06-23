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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CompressedMetaWriterTest {



    @Test
    public void testRecordWrite() {
        int shardId = 0;
        TestUploader result = new TestUploader(shardId);
        CompressedMetaWriter compressedMetaWriter = new CompressedMetaWriter("ssp", result, shardId);
        final long hash = -617261765316316L;
        final String tagSeed = "tag_key``-`````@~~~@tag!!_value*&^))(((";
        final String tag = tagSeed + tagSeed + tagSeed + tagSeed + tagSeed + tagSeed + tagSeed;
        final byte[] tagBytes = tag.getBytes(StandardCharsets.UTF_8);

        final String metric = "metric.jahdajh(())$$$###@d.habcad";
        final byte[] metricBytes = metric.getBytes(StandardCharsets.UTF_8);

        compressedMetaWriter.init((int) Instant.now().getEpochSecond());

        compressedMetaWriter.advanceRecord();
        compressedMetaWriter.writeHash(hash);
        compressedMetaWriter.writeTag(tagBytes);
        compressedMetaWriter.writeMetric(metricBytes);
        compressedMetaWriter.advanceRecord();

        compressedMetaWriter.close();

        int expectedLen = 2 + 2 + 8 + 2 + metricBytes.length + 2 + tagBytes.length;

        assertEquals(expectedLen, result.readLen());

        result.readRecordHeader();

        assertEquals(hash, result.readHash());

        assertEquals(tag, result.readTag(StandardCharsets.UTF_8));

        assertEquals(metric, result.readMetric(StandardCharsets.UTF_8));

    }

    @Test
    public void testOutOfOrderRecordWrite() {
        int shardId = 0;
        TestUploader result = new TestUploader(shardId);
        CompressedMetaWriter compressedMetaWriter = new CompressedMetaWriter("ssp", result, shardId);
        final long hash = -617261765316316L;
        final String tagSeed = "tag_key``-`````@~~~@tag!!_value*&^))(((";
        final String tag = tagSeed + tagSeed + tagSeed + tagSeed + tagSeed + tagSeed + tagSeed;
        final byte[] tagBytes = tag.getBytes(StandardCharsets.UTF_8);

        final String metric = "metric.jahdajh(())$$$###@d.habcad";
        final byte[] metricBytes = metric.getBytes(StandardCharsets.UTF_8);

        compressedMetaWriter.init((int) Instant.now().getEpochSecond());

        compressedMetaWriter.advanceRecord();

        //Different order write
        compressedMetaWriter.writeMetric(metricBytes);

        compressedMetaWriter.writeHash(hash);

        compressedMetaWriter.writeTag(tagBytes);

        compressedMetaWriter.advanceRecord();

        compressedMetaWriter.close();

        int expectedLen = 2 + 2 + 8 + 2 + metricBytes.length + 2 + tagBytes.length;

        assertEquals(expectedLen, result.readLen());

        result.readRecordHeader();

        assertEquals(metric, result.readMetric(StandardCharsets.UTF_8));

        assertEquals(hash, result.readHash());

        assertEquals(tag, result.readTag(StandardCharsets.UTF_8));

    }
}
