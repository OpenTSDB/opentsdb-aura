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
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.time.temporal.ChronoUnit;

public class S3Uploader implements Uploader {

    public static final String M_UPLOAD_TIME = "meta.s3.upload.time";
    public static final String M_UPLOAD_2XX = "meta.s3.upload.2xx";
    public static final String M_UPLOAD_4XX = "meta.s3.upload.4xx";
    public static final String M_UPLOAD_5XX = "meta.s3.upload.5xx";
    public static final String M_UPLOAD_OTHER = "meta.s3.upload.other";
    public static final String M_UPLOAD_RETRIES = "meta.s3.upload.retries";
    public static final String M_UPLOAD_EX = "meta.s3.upload.exceptions";
    public static final String M_UPLOAD_SIZE = "meta.s3.upload.size";

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
    private final StatsCollector stats;
    private final String[] tags;
    private final int retries = 3;

    public S3Uploader(
            final S3Client s3Client,
            final StatsCollector stats,
            final String bucketName,
            final String namespace,
            final int shardid) {

        this.bucketName = bucketName;
        this.namespace = namespace;
        this.shardid = shardid;
        this.format = namespace + "/%s/" + shardid + "-%s" ;
        this.s3Client = s3Client;
        this.stats = stats;
        this.tags = new String[]{
                "bucket_name", bucketName,
                "namespace", namespace,
                "shard_id", String.valueOf(shardid)};

    }

    public void upload(int timestamp, byte[] payload) {

        final String objectName = String.format(this.format, timestamp, index);
        stats.setGauge(M_UPLOAD_SIZE, payload.length, tags);
        log.info("S3 upload for: {} size: {}",
                objectName,
                payload.length);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(objectName).build();
        //To avoid making a byte array copy.

        long start = DateTime.nanoTime();
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
                    stats.addTime(M_UPLOAD_TIME, DateTime.nanoTime() - start, ChronoUnit.NANOS, tags);
                    stats.incrementCounter(M_UPLOAD_RETRIES, tags);
                    start = DateTime.nanoTime();
                    continue;
                }
                stats.addTime(M_UPLOAD_TIME, DateTime.nanoTime() - start, ChronoUnit.NANOS, tags);
                stats.incrementCounter(M_UPLOAD_2XX, tags);
                log.info("S3 uploaded for: {} status code: {} status text: {}",
                        objectName,
                        putObjectResponse.sdkHttpResponse().statusCode(),
                        putObjectResponse.sdkHttpResponse().statusText());
                break;
            } catch (Throwable t) {
                log.error("Error uploading to S3 for {} retries left: {}", objectName, retriesLeft, t);
                retriesLeft--;
                stats.addTime(M_UPLOAD_TIME, DateTime.nanoTime() - start, ChronoUnit.NANOS, tags);
                stats.incrementCounter(M_UPLOAD_EX, tags);
            }
        }

    }

    private void reportErrorResponse(int statusCode) {
        if( statusCode >= 400 && statusCode < 500) {
            stats.incrementCounter(M_UPLOAD_4XX, tags);
        } else if ( statusCode >= 500 && statusCode < 600) {
            stats.incrementCounter(M_UPLOAD_5XX, tags);
        } else {
            stats.incrementCounter(M_UPLOAD_OTHER, tags);
        }
    }

}
