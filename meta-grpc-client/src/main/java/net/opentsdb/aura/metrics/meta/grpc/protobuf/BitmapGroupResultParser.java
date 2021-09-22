package net.opentsdb.aura.metrics.meta.grpc.protobuf;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;

public class BitmapGroupResultParser extends AbstractParser<BitmapGroupResultLite> {
    @Override
    public BitmapGroupResultLite parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {

        try {
            BitmapGroupResultLite groupResult = new BitmapGroupResultLite();
            TimeseriesParser timeseriesParser = new TimeseriesParser(groupResult);
            boolean done = false;
            while (!done) {
                int tag = input.readTag();
                switch (tag) {
                    case 0:
                        done = true;
                        break;
                    case 8: {
                        groupResult.addTagHash(input.readInt64());
                        break;
                    }
                    case 10: {
                        int length = input.readRawVarint32();
                        int limit = input.pushLimit(length);
                        while (input.getBytesUntilLimit() > 0) {
                            groupResult.addTagHash(input.readInt64());
                        }
                        input.popLimit(limit);
                        break;
                    }
                    case 18: {
                        input.readMessage(timeseriesParser, extensionRegistry);
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
            return groupResult;
        } catch (IOException e) {
            e.printStackTrace();
            throw new InvalidProtocolBufferException(e);
        }
    }
}
