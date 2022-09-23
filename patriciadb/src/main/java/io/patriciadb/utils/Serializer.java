package io.patriciadb.utils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public interface Serializer<E> {

    void serialize(E entry, ByteArrayOutputStream bos);

    E deserialize(ByteBuffer buffer);

    default ByteBuffer serialize(E entry) {
        var bos = new ByteArrayOutputStream();
        serialize(entry, bos);
        return ByteBuffer.wrap(bos.toByteArray());
    }
}
