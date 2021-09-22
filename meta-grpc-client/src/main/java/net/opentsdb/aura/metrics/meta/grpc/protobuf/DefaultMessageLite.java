package net.opentsdb.aura.metrics.meta.grpc.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import java.io.OutputStream;

public interface DefaultMessageLite extends MessageLite {

    @Override
    default void writeTo(CodedOutputStream output){
        throw new UnsupportedOperationException();
    }

    @Override
    default int getSerializedSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    default Parser<? extends MessageLite> getParserForType() {
        throw new UnsupportedOperationException();
    }

    @Override
    default ByteString toByteString() {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte[] toByteArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeTo(OutputStream output) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void writeDelimitedTo(OutputStream output) {
        throw new UnsupportedOperationException();
    }

    @Override
    default MessageLite.Builder newBuilderForType() {
        throw new UnsupportedOperationException();
    }

    @Override
    default MessageLite.Builder toBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    default MessageLite getDefaultInstanceForType() {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean isInitialized() {
        throw new UnsupportedOperationException();
    }
}
