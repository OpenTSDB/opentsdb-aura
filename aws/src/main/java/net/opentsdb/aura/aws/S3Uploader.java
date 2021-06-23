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

import net.opentsdb.aura.metrics.metaflush.Uploader;
import io.ultrabrew.metrics.Counter;
import io.ultrabrew.metrics.Gauge;
import io.ultrabrew.metrics.MetricRegistry;
import io.ultrabrew.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;

public class S3Uploader implements Uploader {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String bucketName;
    private final String namespace;
    private final int shardid;

    /**
     * format: namespace/timestamp/shardId-index
     */
    private final String format;
    private int index = 0;
    private final S3Client s3Client;
    private final Timer s3RequestTimer;
    private final Gauge payloadSize;
    private final Counter s3_2xx;
    private final Counter s3_4xx;
    private final Counter s3_5xx;
    private final Counter s3_other;
    private final Counter s3_exceptions;
    private final String[] tags;
    private final int retries = 3;
    public S3Uploader(
            final S3Client s3Client,
            final MetricRegistry metricRegistry,
            final String bucketName,
            final String namespace,
            final int shardid) {

        this.bucketName = bucketName;
        this.namespace = namespace;
        this.shardid = shardid;
        this.format = namespace + "/%s/" + shardid + "-%s" ;
        this.s3Client = s3Client;
        this.s3RequestTimer = metricRegistry.timer("meta.s3.upload.time");
        this.s3_2xx = metricRegistry.counter("meta.s3.upload.2xx");
        this.s3_4xx = metricRegistry.counter("meta.s3.upload.4xx");
        this.s3_5xx = metricRegistry.counter("meta.s3.upload.5xx");
        this.s3_other = metricRegistry.counter("meta.s3.upload.other");
        this.s3_exceptions = metricRegistry.counter("meta.s3.upload.exceptions");
        this.payloadSize = metricRegistry.gauge("meta.s3.upload.size");
        this.tags = new String[]{
                "bucket_name", bucketName,
                "namespace", namespace,
                "shard_id", String.valueOf(shardid)};

    }

    public void upload(int timestamp, byte[] payload) {

        final String objectName = String.format(this.format, timestamp, index);
        this.payloadSize.set(payload.length, tags);
        log.info("S3 upload for: {} size: {}",
                objectName,
                payload.length);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(objectName).build();
        //To avoid making a byte array copy.

        long start = this.s3RequestTimer.start();
        int retriesLeft = this.retries;
        while(retriesLeft > 0) {
            try {
                final PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest,
                        RequestBody.fromContentProvider(() -> new ByteArrayInputStream(payload), payload.length, Mimetype.MIMETYPE_OCTET_STREAM));

                if (!putObjectResponse.sdkHttpResponse().isSuccessful()) {
                    //failed
                    log.error("Non 2xx error for uploading to S3: {} {} retries left: {}"
                            , putObjectResponse.sdkHttpResponse().statusCode()
                            , putObjectResponse.sdkHttpResponse().statusText()
                            , retriesLeft
                    );
                    reportErrorResponse(putObjectResponse.sdkHttpResponse().statusCode());
                    retriesLeft--;
                    this.s3RequestTimer.stop(start, tags);
                    start = this.s3RequestTimer.start();
                    continue;
                }
                this.s3RequestTimer.stop(start, tags);
                this.s3_2xx.inc(tags);
                log.info("S3 uploaded for: {} status code: {} status text: {}",
                        objectName,
                        putObjectResponse.sdkHttpResponse().statusCode(),
                        putObjectResponse.sdkHttpResponse().statusText());
                break;
            } catch (Throwable t) {
                log.error("Error uploading to S3 for {} retries left: {}", objectName, retriesLeft, t);
                retriesLeft--;
                this.s3RequestTimer.stop(start, tags);
                start = this.s3RequestTimer.start();
                this.s3_exceptions.inc(tags);
            }
        }

    }

    private void reportErrorResponse(int statusCode) {
        if( statusCode >= 400 && statusCode < 500) {
            this.s3_4xx.inc(tags);
        } else if ( statusCode >= 500 && statusCode < 600) {
            this.s3_5xx.inc(tags);
        } else {
            this.s3_other.inc(tags);
        }
    }

}
