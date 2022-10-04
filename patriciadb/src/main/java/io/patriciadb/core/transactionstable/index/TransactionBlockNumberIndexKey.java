package io.patriciadb.core.transactionstable.index;

import io.patriciadb.utils.Serializer;
import io.patriciadb.utils.VarInt;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public record TransactionBlockNumberIndexKey(long blockId, long primaryKey) implements Comparable<TransactionBlockNumberIndexKey> {

    public static final Serializer<TransactionBlockNumberIndexKey> SERIALIZER = new SnapshotIdIndexKeySerializer();

    @Override
    public int compareTo(TransactionBlockNumberIndexKey o) {
        int c= Long.compare(blockId, o.blockId);
        if(c!=0) return c;
        return Long.compare(primaryKey, o.primaryKey);
    }

    private static class SnapshotIdIndexKeySerializer implements Serializer<TransactionBlockNumberIndexKey> {
        private  SnapshotIdIndexKeySerializer() {

        }

        @Override
        public void serialize(TransactionBlockNumberIndexKey entry, ByteArrayOutputStream bos) {
            VarInt.putVarLong16(entry.blockId, bos);
            VarInt.putVarLong16(entry.primaryKey, bos);
        }

        @Override
        public TransactionBlockNumberIndexKey deserialize(ByteBuffer buffer) {
            long blockId =VarInt.getVarLong16(buffer);
            long primaryKey = VarInt.getVarLong16(buffer);
            return new TransactionBlockNumberIndexKey(blockId, primaryKey);
        }
    }
}
