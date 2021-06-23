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

package net.opentsdb.aura.aws.auth;

import com.yahoo.athenz.zts.AWSTemporaryCredentials;
import com.yahoo.athenz.zts.ZTSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import javax.net.ssl.SSLContext;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Uses the Zts client directly to create
 */
public class AthensCredentialsProviderImpl implements AwsCredentialsProvider {

    private static final Logger log = LoggerFactory.getLogger(AthensCredentialsProviderImpl.class);

    private final ZTSClient ztsClient;
    private final String domainName;
    private final String rolename;
    private final Integer expiryTimeInSecs;
    private volatile long credTimeSecs;
    private volatile AwsCredentials awsCredentials;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public AthensCredentialsProviderImpl(Builder builder) {

        this.ztsClient = new ZTSClient(builder.ztsUrl, builder.sslContext);
        this.domainName = builder.domainName;
        this.rolename = builder.roleName;
        this.expiryTimeInSecs = builder.expiryTimeInSecs;
    }


    @Override
    public AwsCredentials resolveCredentials() {
        boolean releaseReadLock = true;
        lock.readLock().lock();
        try {
            if (shouldRefresh()) {
                lock.readLock().unlock();
                releaseReadLock = false;
                lock.writeLock().lock();
                try {
                    if(shouldRefresh()) {
                        final AWSTemporaryCredentials awsTemporaryCredentials = ztsClient
                                .getAWSTemporaryCredentials(
                                        domainName,
                                        rolename,
                                        null,
                                        null,
                                        expiryTimeInSecs,
                                        true);

                        this.credTimeSecs = awsTemporaryCredentials.getExpiration().millis() / 1_000;
                        this.awsCredentials = AwsSessionCredentials.create(
                                awsTemporaryCredentials.getAccessKeyId(),
                                awsTemporaryCredentials.getSecretAccessKey(),
                                awsTemporaryCredentials.getSessionToken()
                        );
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
            return this.awsCredentials;
        } finally {
            if (releaseReadLock)
                lock.readLock().unlock();
        }
    }

    private boolean shouldRefresh() {
        return (awsCredentials == null || (credTimeSecs - System.currentTimeMillis()) <= 10 );
    }


    public static class Builder {
        private String ztsUrl;
        private SSLContext sslContext;
        private String domainName;
        private String roleName;
        private Integer expiryTimeInSecs;

        public Builder ztsUrl(String ztsUrl) {
            this.ztsUrl = ztsUrl;
            return this;
        }

        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        public Builder domainName(String domainName) {
            this.domainName = domainName;
            return this;
        }

        public Builder roleName(String roleName) {
            this.roleName = roleName;
            return this;
        }

        public Builder expiryTimeInSecs(Integer expiryTimeInSecs) {
            this.expiryTimeInSecs = expiryTimeInSecs;
            return this;
        }

        public AthensCredentialsProviderImpl build() {

            return new AthensCredentialsProviderImpl(this);
        }

        public static Builder create() {
            return new Builder();
        }

    }
}
