package io.patriciadb.utils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BosUtils {

    public static void writeBytes(byte[] data, ByteArrayOutputStream bos) {
        VarInt.putVarInt(data.length, bos);
        bos.writeBytes(data);
    }

    public static byte[] readBytes(ByteBuffer buffer) {
        int size = VarInt.getVarInt(buffer);
        byte[] data = new byte[size];
        buffer.get(data);
        return data;
    }

    public static void writeString(String string, ByteArrayOutputStream bos) {
        var data = string.getBytes(StandardCharsets.UTF_8);
        writeBytes(data, bos);
    }

    public static String readString(ByteBuffer buffer) {
        return new String(readBytes(buffer));
    }
}
