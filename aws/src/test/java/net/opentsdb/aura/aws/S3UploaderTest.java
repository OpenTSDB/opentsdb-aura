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

package net.opentsdb.aura.aws;

import io.ultrabrew.metrics.MetricRegistry;
import mockit.Expectations;
import org.junit.jupiter.api.Test;
import mockit.Injectable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.time.Instant;


public class S3UploaderTest {

    @Injectable
    S3Client s3Client;
    @Injectable
    MetricRegistry metricRegistry;

    @Test
    public void simpleTest() {

        final String bucketName = "test-myst";
        final String namespace = "ssp";
        final int shardid = 0;
        final int timestamp = (int) Instant.now().getEpochSecond();
        final String objectName = namespace + "/" + timestamp + "/" + shardid + "-0";
        byte[] payload = new byte[1];
        final PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(objectName).build();
        S3Uploader s3Uploader = new S3Uploader(s3Client, metricRegistry, bucketName, namespace, shardid);
        new Expectations() {{

            s3Client.putObject(request, withInstanceOf(RequestBody.class));
            times = 1;

        }};
        s3Uploader.upload(timestamp, payload);
    }
}
