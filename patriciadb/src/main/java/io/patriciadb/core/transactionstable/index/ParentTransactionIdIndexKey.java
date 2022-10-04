package io.patriciadb.core.transactionstable.index;

import io.patriciadb.utils.Serializer;
import io.patriciadb.utils.VarInt;
import io.patriciadb.utils.BosUtils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public record ParentTransactionIdIndexKey(byte[] parentBlockHash, long primaryKey) implements Comparable<ParentTransactionIdIndexKey> {

    public static final Serializer<ParentTransactionIdIndexKey> SERIALIZER = new SnapshotIdIndexKeySerializer();

    @Override
    public int compareTo(ParentTransactionIdIndexKey o) {
        int c= Arrays.compareUnsigned(parentBlockHash, o.parentBlockHash);
        if(c!=0) return c;
        return Long.compare(primaryKey, o.primaryKey);
    }

    private static class SnapshotIdIndexKeySerializer implements Serializer<ParentTransactionIdIndexKey> {
        private  SnapshotIdIndexKeySerializer() {

        }

        @Override
        public void serialize(ParentTransactionIdIndexKey entry, ByteArrayOutputStream bos) {
            BosUtils.writeBytes(entry.parentBlockHash, bos);
            VarInt.putVarLong16(entry.primaryKey, bos);
        }

        @Override
        public ParentTransactionIdIndexKey deserialize(ByteBuffer buffer) {
            byte[] snapshotid =BosUtils.readBytes(buffer);
            long primaryKey = VarInt.getVarLong16(buffer);
            return new ParentTransactionIdIndexKey(snapshotid, primaryKey);
        }
    }
}
