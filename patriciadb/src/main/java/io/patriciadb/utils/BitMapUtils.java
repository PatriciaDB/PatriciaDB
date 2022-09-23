package io.patriciadb.utils;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;

public class BitMapUtils {

    public static byte[] serialize(Roaring64NavigableMap bitmap) {
        try {
            bitmap.trim();
            bitmap.runOptimize();
            var bos = new ByteArrayOutputStream();
            var dos = new DataOutputStream(bos);
            bitmap.serialize(dos);
            dos.flush();
            return bos.toByteArray();
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Roaring64NavigableMap deserialize(byte[] data) {
        try {
            var bis = new ByteArrayInputStream(data);
            var dis = new DataInputStream(bis);
            Roaring64NavigableMap bitset = new Roaring64NavigableMap();
            bitset.deserialize(dis);
            return bitset;
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
