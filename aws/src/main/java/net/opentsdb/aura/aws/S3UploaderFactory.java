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
import net.opentsdb.aura.metrics.metaflush.UploaderFactory;
import net.opentsdb.stats.StatsCollector;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class S3UploaderFactory implements UploaderFactory {

    private final Map<Integer,S3Uploader> uploaders;
    public S3UploaderFactory(Builder builder) {
        final int[] shardIds = builder.shardIds;
        this.uploaders =  new HashMap<>();

        for(int i = 0; i < shardIds.length; i++) {
            S3ClientBuilder s3ClientBuilder = S3Client.builder()
                    .region(builder.region)
                    .credentialsProvider(builder.awsCredentialsProvider);
            if (builder.endpoint != null) {
                s3ClientBuilder.endpointOverride(builder.endpoint);
            }
            final S3Client client = s3ClientBuilder.build();

            this.uploaders.put(shardIds[i], new S3Uploader(client, builder.stats, builder.bucketName, builder.namespace, shardIds[i]));
        }
    }

    @Override
    public Uploader create(int shardId) {
        return this.uploaders.get(shardId);
    }

    public static class Builder {

        private String bucketName;
        private String namespace;
        private int[] shardIds;
        private Region region;
        private URI endpoint;
        private AwsCredentialsProvider awsCredentialsProvider;
        private StatsCollector stats;

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }
        public Builder region(String region) {
            this.region = Region.of(region);
            return this;
        }

        public Builder numShards(int[] shardIds) {
            this.shardIds = shardIds;
            return this;
        }

        public Builder endpoint(URI endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder awsCredentialsProvider(AwsCredentialsProvider awsCredentialsProvider) {
            this.awsCredentialsProvider = awsCredentialsProvider;
            return this;
        }

        public Builder withStatsCollector(StatsCollector stats) {
            this.stats = stats;
            return this;
        }

        public S3UploaderFactory build() {
            return new S3UploaderFactory(this);
        }

        public static Builder create() {
            return new Builder();
        }

    }
}
