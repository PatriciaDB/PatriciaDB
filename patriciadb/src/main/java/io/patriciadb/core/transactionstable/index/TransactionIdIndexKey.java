package io.patriciadb.core.transactionstable.index;

import io.patriciadb.utils.VarInt;
import io.patriciadb.utils.Serializer;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public record TransactionIdIndexKey(byte[] transactionId,
                                    long primaryKey) implements Comparable<TransactionIdIndexKey> {
    public static final Serializer<TransactionIdIndexKey> SERIALIZER = new SnapshotIdIndexKeySerializer();

    @Override
    public int compareTo(TransactionIdIndexKey o) {
        int c = Arrays.compareUnsigned(transactionId, o.transactionId);
        if (c != 0) return c;
        return Long.compare(primaryKey, o.primaryKey);
    }

    private static class SnapshotIdIndexKeySerializer implements Serializer<TransactionIdIndexKey> {
        private SnapshotIdIndexKeySerializer() {

        }

        @Override
        public void serialize(TransactionIdIndexKey entry, ByteArrayOutputStream bos) {
            VarInt.putVarInt(entry.transactionId.length, bos);
            bos.writeBytes(entry.transactionId);
            VarInt.putVarLong16(entry.primaryKey, bos);
        }

        @Override
        public TransactionIdIndexKey deserialize(ByteBuffer buffer) {
            int keyLength = VarInt.getVarInt(buffer);
            byte[] transactionId = new byte[keyLength];
            try {
                buffer.get(transactionId);
            } catch (BufferUnderflowException ex) {
                System.out.println("Something dodge");
            }
            long primaryKey = VarInt.getVarLong16(buffer);
            return new TransactionIdIndexKey(transactionId, primaryKey);
        }
    }
}
