package io.patriciadb.core.transactionstable;

import io.patriciadb.table.Entity;
import io.patriciadb.utils.VarInt;
import io.patriciadb.utils.BosUtils;
import io.patriciadb.utils.Serializer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;

public class TransactionEntity implements Entity {

    public static final Serializer<TransactionEntity> SERIALIZER = new SnapshotEntitySerializer();
    private long primaryKey;
    private byte[] transactionId;
    private byte[] parentTransactionId;
    private  long blockNumber;
    private  Instant creationTime;
    private  byte[] extra;
    private long indexRootNodeId;
    private byte[] lostNodeIds;
    private byte[] newNodeIds;

    @Override
    public long getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public void setPrimaryKey(long primaryKey) {
        this.primaryKey = primaryKey;
    }

    public byte[] getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(byte[] transactionId) {
        this.transactionId = transactionId;
    }

    public byte[] getParentTransactionId() {
        return parentTransactionId;
    }

    public void setParentTransactionId(byte[] parentTransactionId) {
        this.parentTransactionId = parentTransactionId;
    }

    public long getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(long blockNumber) {
        this.blockNumber = blockNumber;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Instant creationTime) {
        this.creationTime = creationTime;
    }

    public byte[] getExtra() {
        return extra;
    }

    public void setExtra(byte[] extra) {
        this.extra = extra;
    }

    public long getIndexRootNodeId() {
        return indexRootNodeId;
    }

    public void setIndexRootNodeId(long indexRootNodeId) {
        this.indexRootNodeId = indexRootNodeId;
    }

    public byte[] getLostNodeIds() {
        return lostNodeIds;
    }

    public void setLostNodeIds(byte[] lostNodeIds) {
        this.lostNodeIds = lostNodeIds;
    }

    public byte[] getNewNodeIds() {
        return newNodeIds;
    }

    public void setNewNodeIds(byte[] newNodeIds) {
        this.newNodeIds = newNodeIds;
    }

    @Override
    public String toString() {
        return "BlockEntity{" +
                "primaryKey=" + primaryKey +
                ", transactionId=" + Arrays.toString(transactionId) +
                ", parentTransactionId=" + Arrays.toString(parentTransactionId) +
                ", blockNumber=" + blockNumber +
                ", creationTime=" + creationTime +
                ", extra='" + extra.length + '\'' +
                ", indexRootNodeId=" + indexRootNodeId +
                ", lostNodeIds=" + lostNodeIds.length +
                ", newNodeIds=" + newNodeIds.length +
                '}';
    }

    private static class SnapshotEntitySerializer implements Serializer<TransactionEntity> {
        @Override
        public void serialize(TransactionEntity entry, ByteArrayOutputStream bos) {
            BosUtils.writeBytes(entry.transactionId, bos);
            BosUtils.writeBytes(entry.parentTransactionId, bos);
            VarInt.putVarLong16(entry.blockNumber, bos);
            VarInt.putVarLong16(entry.creationTime.toEpochMilli(), bos);
            BosUtils.writeBytes(entry.extra, bos);
            VarInt.putVarLong16(entry.indexRootNodeId, bos);
            BosUtils.writeBytes(entry.newNodeIds, bos);
            BosUtils.writeBytes(entry.lostNodeIds, bos);
        }

        @Override
        public TransactionEntity deserialize(ByteBuffer buffer) {
            var entity =new TransactionEntity();

            entity.transactionId = BosUtils.readBytes(buffer);
            entity.parentTransactionId = BosUtils.readBytes(buffer);
            entity.blockNumber = VarInt.getVarLong16(buffer);
            entity.creationTime = Instant.ofEpochMilli(VarInt.getVarLong16(buffer));
            entity.extra = BosUtils.readBytes(buffer);
            entity.indexRootNodeId = VarInt.getVarLong16(buffer);
            entity.newNodeIds = BosUtils.readBytes(buffer);
            entity.lostNodeIds = BosUtils.readBytes(buffer);
            return entity;
        }
    }
}
