package io.patriciadb.utils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class Serializers {

    public static final Serializer<Long> LONG_SERIALIZER = new LongSerializer();


    private static final class LongSerializer implements Serializer<Long> {
        @Override
        public void serialize(Long entry, ByteArrayOutputStream bos) {
            VarInt.putVarLong16(entry, bos);
        }

        @Override
        public Long deserialize(ByteBuffer buffer) {
            return VarInt.getVarLong16(buffer);
        }
    }
}
