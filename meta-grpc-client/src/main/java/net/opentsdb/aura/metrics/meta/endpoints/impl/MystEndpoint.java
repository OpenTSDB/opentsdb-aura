package net.opentsdb.aura.metrics.meta.endpoints.impl;

import net.opentsdb.aura.metrics.meta.endpoints.ShardEndPoint;
//TODO: Make a builder
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
}
