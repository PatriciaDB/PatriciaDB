package io.patriciadb.core.blocktable.index;

import io.patriciadb.utils.Serializer;
import io.patriciadb.utils.VarInt;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public record BlockIdIndexKey(long blockId, long primaryKey) implements Comparable<BlockIdIndexKey> {

    public static final Serializer<BlockIdIndexKey> SERIALIZER = new SnapshotIdIndexKeySerializer();

    @Override
    public int compareTo(BlockIdIndexKey o) {
        int c= Long.compare(blockId, o.blockId);
        if(c!=0) return c;
        return Long.compare(primaryKey, o.primaryKey);
    }

    private static class SnapshotIdIndexKeySerializer implements Serializer<BlockIdIndexKey> {
        private  SnapshotIdIndexKeySerializer() {

        }

        @Override
        public void serialize(BlockIdIndexKey entry, ByteArrayOutputStream bos) {
            VarInt.putVarLong16(entry.blockId, bos);
            VarInt.putVarLong16(entry.primaryKey, bos);
        }

        @Override
        public BlockIdIndexKey deserialize(ByteBuffer buffer) {
            long blockId =VarInt.getVarLong16(buffer);
            long primaryKey = VarInt.getVarLong16(buffer);
            return new BlockIdIndexKey(blockId, primaryKey);
        }
    }
}
