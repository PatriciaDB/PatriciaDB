package io.patriciadb.index.btree;

import io.patriciadb.utils.Serializer;
import io.patriciadb.utils.VarInt;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class LongSerializer implements Serializer<Long> {
    public static final LongSerializer INSTANCE = new LongSerializer();
    @Override
    public void serialize(Long entry, ByteArrayOutputStream bos) {
        VarInt.putVarLong(entry, bos);
    }

    @Override
    public Long deserialize(ByteBuffer buffer) {
        return VarInt.getVarLong(buffer);
    }
}
