package net.opentsdb.aura.metrics.meta.grpc;

import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

public class TestUtil {
    public static int[] getTimestampSequence(int count) {
        final int epochSecond = (int) Instant.now().getEpochSecond();
        final int[] array_count = new int[count];
        array_count[0] = epochSecond - epochSecond % 3600 * 6;

        for(int i = 1 ; i < count; i++) {
            array_count[i] = array_count[i-1] - 3600 * 6;
        }

        return array_count;
    }

    public static void verifyBitMap(RoaringBitmap bitMap, int[] ts) {
        final Iterator<Integer> iterator = bitMap.iterator();
        int i = 0;
        for (; i < ts.length; i++) {
            assertTrue(bitMap.contains(ts[i]));
        }
        assertEquals(i, ts.length);
    }

    public static byte[] getBitMap(int[] ts)  {
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        roaringBitmap.add(ts);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        try {
            roaringBitmap.serialize(dataOutputStream);
            dataOutputStream.flush();
        } catch (IOException e) {
            fail("Serializing Roaring bitmap failed", e);
        }


        return byteArrayOutputStream.toByteArray();
    }
}
