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
package net.opentsdb.aura.metrics.meta.endpoints.impl;

import net.opentsdb.aura.metrics.meta.endpoints.ShardEndPoint;

import java.util.Objects;


public class SimpleEndPoint implements ShardEndPoint {

    private final String host;
    private final int port;
    private final int shardIndex;

    public SimpleEndPoint(String host, int port, int shardIndex) {
        this.host = host;
        this.port = port;
        this.shardIndex = shardIndex;
    }


    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public int getShardIndex() {
        return this.shardIndex;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.http2_0;
    }

    @Override
    public boolean equals(Object obj){
        if (Objects.isNull(obj)){
            return false;
        }
        if (this == obj){
            return  true;
        }else if (!(obj instanceof ShardEndPoint)) {
            return false;

        }else{
            ShardEndPoint that = (ShardEndPoint) obj; 
            if (Objects.equals(this.getHost(), that.getHost()) &&
                    Objects.equals(this.getPort(), that.getPort())
                    && Objects.equals(this.getProtocol(), that.getProtocol()) &&
                    Objects.equals(this.mtls(), that.mtls())){
                        return true;
            }else{
                        return false;
            }
        }
}


    @Override
    public boolean mtls() {
        return false;
    }

    public static class Builder {
        private String l_host;
        private int l_port;
        private int l_shardIndex;

        public Builder withShardIndex(int shardIndex) {
            this.l_shardIndex = shardIndex;
            return this;
        }

        public Builder withHost(String host) {
            this.l_host = host;
            return this;
        }

        public Builder withPort(int port) {
            this.l_port = port;
            return this;
        }

        public SimpleEndPoint build() {
            return new SimpleEndPoint(l_host, l_port, l_shardIndex);
        }

        public static Builder newBuilder() {
            return new Builder();
        }

    }
    @Override
    public String toString() {
        return "Endpoints {host=" + host + ", port=" + port +"}";
    }
}
