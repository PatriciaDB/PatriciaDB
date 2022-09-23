package io.patriciadb.index.btree;

import io.patriciadb.utils.Serializer;
import io.patriciadb.utils.VarInt;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class AccountIndex implements Comparable<AccountIndex> {
    public static final Serializer<AccountIndex> SERIALIZER = new AccountSerializer();

    private final long accountId;
    private final long blockId;

    public AccountIndex(long accountId, long blockId) {
        this.accountId = accountId;
        this.blockId = blockId;
    }

    public long getAccountId() {
        return accountId;
    }

    public long getBlockId() {
        return blockId;
    }

    @Override
    public int compareTo(AccountIndex o) {
        int c = Long.compare(accountId, o.accountId);
        if (c != 0) return c;
        return Long.compare(blockId, o.blockId);
    }

    @Override
    public String toString() {
        return "AccountIndex{" +
                "accountId=" + accountId +
                ", blockId=" + blockId +
                '}';
    }

    public static class AccountSerializer implements Serializer<AccountIndex> {

        @Override
        public void serialize(AccountIndex entry, ByteArrayOutputStream bos) {
            VarInt.putVarLong(entry.getAccountId(),bos);
            VarInt.putVarLong(entry.getBlockId(),bos);
        }

        @Override
        public AccountIndex deserialize(ByteBuffer buffer) {
            long accountId = VarInt.getVarLong(buffer);
            long blockId = VarInt.getVarLong(buffer);
            return new AccountIndex(accountId, blockId);
        }
    }
}
