package net.opentsdb.aura.metrics.meta.grpc.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import java.io.OutputStream;

public class TimeseriesLite implements MessageLite {
    @Override
    public void writeTo(CodedOutputStream output){
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSerializedSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Parser<? extends MessageLite> getParserForType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteString toByteString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toByteArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(OutputStream output) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDelimitedTo(OutputStream output) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Builder newBuilderForType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Builder toBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessageLite getDefaultInstanceForType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInitialized() {
        throw new UnsupportedOperationException();
    }
}
