package net.opentsdb.aura.metrics.meta;

public class MetaFetchException extends Exception {

    public MetaFetchException(String message, Exception e) {
        super(message, e);
    }
}
