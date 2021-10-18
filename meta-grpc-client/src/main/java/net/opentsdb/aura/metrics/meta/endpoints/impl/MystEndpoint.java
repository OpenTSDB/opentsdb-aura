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


public class MystEndpoint implements ShardEndPoint {

    private final String host;
    private final int port;

    public MystEndpoint(String host, int port) {
        this.host = host;
        this.port = port;
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
    public Protocol getProtocol() {
        return Protocol.http2_0;
    }

    @Override
    public boolean equals(Object that){
        if (Objects.isNull(that)){
            return false;
        }
        if (this == that){
            return  true;
        }else if (!(that instanceof ShardEndPoint)) {
            return false;

        }else{
            if (Objects.equals(this.getHost(), ((ShardEndPoint) that).getHost()) &&
                    Objects.equals(this.getPort(), ((ShardEndPoint) that).getPort())
                    && Objects.equals(this.getProtocol(), ((ShardEndPoint) that).getProtocol()) &&
                    ((ShardEndPoint) that).mtls() == false && this.mtls() == false ){

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

        public Builder withHost(String host) {
            this.l_host = host;
            return this;
        }

        public Builder withPort(int port) {
            this.l_port = port;
            return this;
        }

        public MystEndpoint build() {
            return new MystEndpoint(l_host, l_port);
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
