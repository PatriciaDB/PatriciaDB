package io.patriciadb.core.blocktable.index;

import io.patriciadb.utils.Serializer;
import io.patriciadb.utils.VarInt;
import io.patriciadb.utils.BosUtils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public record ParentBlockHashIndexKey(byte[] parentBlockHash, long primaryKey) implements Comparable<ParentBlockHashIndexKey> {

    public static final Serializer<ParentBlockHashIndexKey> SERIALIZER = new SnapshotIdIndexKeySerializer();

    @Override
    public int compareTo(ParentBlockHashIndexKey o) {
        int c= Arrays.compareUnsigned(parentBlockHash, o.parentBlockHash);
        if(c!=0) return c;
        return Long.compare(primaryKey, o.primaryKey);
    }

    private static class SnapshotIdIndexKeySerializer implements Serializer<ParentBlockHashIndexKey> {
        private  SnapshotIdIndexKeySerializer() {

        }

        @Override
        public void serialize(ParentBlockHashIndexKey entry, ByteArrayOutputStream bos) {
            BosUtils.writeBytes(entry.parentBlockHash, bos);
            VarInt.putVarLong16(entry.primaryKey, bos);
        }

        @Override
        public ParentBlockHashIndexKey deserialize(ByteBuffer buffer) {
            byte[] snapshotid =BosUtils.readBytes(buffer);
            long primaryKey = VarInt.getVarLong16(buffer);
            return new ParentBlockHashIndexKey(snapshotid, primaryKey);
        }
    }
}
