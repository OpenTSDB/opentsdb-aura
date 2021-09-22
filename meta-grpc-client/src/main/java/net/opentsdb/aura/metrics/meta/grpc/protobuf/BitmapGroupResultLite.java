package net.opentsdb.aura.metrics.meta.grpc.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import net.opentsdb.aura.metrics.meta.DefaultMetaTimeSeriesQueryResult;

import java.io.OutputStream;

public class BitmapGroupResultLite extends DefaultMetaTimeSeriesQueryResult.DefaultGroupResult implements DefaultMessageLite {

}

