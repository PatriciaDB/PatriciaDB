package io.patriciadb.core.blocktable.index;

import io.patriciadb.utils.VarInt;
import io.patriciadb.utils.Serializer;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public record BlockHashIndexKey(byte[] snapshotId, long primaryKey) implements Comparable<BlockHashIndexKey>{
    public static final Serializer<BlockHashIndexKey> SERIALIZER = new SnapshotIdIndexKeySerializer();

    @Override
    public int compareTo(BlockHashIndexKey o) {
        int c= Arrays.compareUnsigned(snapshotId, o.snapshotId);
        if(c!=0) return c;
        return Long.compare(primaryKey, o.primaryKey);
    }

    private static class SnapshotIdIndexKeySerializer implements Serializer<BlockHashIndexKey> {
        private  SnapshotIdIndexKeySerializer() {

        }

        @Override
        public void serialize(BlockHashIndexKey entry, ByteArrayOutputStream bos) {
            VarInt.putVarInt(entry.snapshotId.length, bos);
            bos.writeBytes(entry.snapshotId);
            VarInt.putVarLong16(entry.primaryKey, bos);
        }

        @Override
        public BlockHashIndexKey deserialize(ByteBuffer buffer) {
            int keyLength = VarInt.getVarInt(buffer);
            byte[] snapshotid =new byte[keyLength];
            try {
                buffer.get(snapshotid);
            }catch (BufferUnderflowException ex) {
                System.out.println("Something dodge");
            }
            long primaryKey = VarInt.getVarLong16(buffer);
            return new BlockHashIndexKey(snapshotid, primaryKey);
        }
    }
}
