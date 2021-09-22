package net.opentsdb.aura.metrics.meta.grpc.protobuf;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;

public class TimeseriesParser extends AbstractParser<TimeseriesLite> {

    private final BitmapGroupResultLite bitmapGroupResultLite;

    public TimeseriesParser(BitmapGroupResultLite bitmapGroupResultLite) {
        this.bitmapGroupResultLite = bitmapGroupResultLite;
    }

    @Override
    public TimeseriesLite parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {

        boolean done = false;
        try {
            long hash = -1;
            while (!done) {
                int tag = input.readTag();
                switch (tag) {
                    case 0:
                        done = true;
                        break;
                    case 8: {
                        hash = input.readInt64();
                        this.bitmapGroupResultLite.addHash(hash);
                        break;
                    }
                    case 18: {
                        byte[] bytes = input.readBytes().toByteArray();
                        this.bitmapGroupResultLite.addBitMap(hash, bytes);
                        break;
                    }
                    default: {
                        if (!input.skipField(tag)) {
                            done = true;
                        }
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new InvalidProtocolBufferException(e);
        }

        return null;
    }
}
