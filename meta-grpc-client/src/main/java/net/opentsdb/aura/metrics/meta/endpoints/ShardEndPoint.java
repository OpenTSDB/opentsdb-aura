package net.opentsdb.aura.metrics.meta.endpoints;

public interface ShardEndPoint {

    String getHost();

    int getPort();

    Protocol getProtocol();

    boolean mtls();

    enum Protocol {
        http1_1, http2_0, https1_1, https_2_0
    }
}
