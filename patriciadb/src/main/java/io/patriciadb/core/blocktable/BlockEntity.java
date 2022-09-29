package io.patriciadb.core.blocktable;

import io.patriciadb.table.Entity;
import io.patriciadb.utils.VarInt;
import io.patriciadb.utils.BosUtils;
import io.patriciadb.utils.Serializer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;

public class BlockEntity implements Entity {

    public static final Serializer<BlockEntity> SERIALIZER = new SnapshotEntitySerializer();
    private long primaryKey;
    private byte[] blockHash;
    private byte[] parentBlockHash;
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

    public byte[] getBlockHash() {
        return blockHash;
    }

    public void setBlockHash(byte[] blockHash) {
        this.blockHash = blockHash;
    }

    public byte[] getParentBlockHash() {
        return parentBlockHash;
    }

    public void setParentBlockHash(byte[] parentBlockHash) {
        this.parentBlockHash = parentBlockHash;
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
                ", blockHash=" + Arrays.toString(blockHash) +
                ", parentBlockHash=" + Arrays.toString(parentBlockHash) +
                ", blockNumber=" + blockNumber +
                ", creationTime=" + creationTime +
                ", extra='" + extra.length + '\'' +
                ", indexRootNodeId=" + indexRootNodeId +
                ", lostNodeIds=" + lostNodeIds.length +
                ", newNodeIds=" + newNodeIds.length +
                '}';
    }

    private static class SnapshotEntitySerializer implements Serializer<BlockEntity> {
        @Override
        public void serialize(BlockEntity entry, ByteArrayOutputStream bos) {
            BosUtils.writeBytes(entry.blockHash, bos);
            BosUtils.writeBytes(entry.parentBlockHash, bos);
            VarInt.putVarLong16(entry.blockNumber, bos);
            VarInt.putVarLong16(entry.creationTime.toEpochMilli(), bos);
            BosUtils.writeBytes(entry.extra, bos);
            VarInt.putVarLong16(entry.indexRootNodeId, bos);
            BosUtils.writeBytes(entry.newNodeIds, bos);
            BosUtils.writeBytes(entry.lostNodeIds, bos);
        }

        @Override
        public BlockEntity deserialize(ByteBuffer buffer) {
            var entity =new BlockEntity();

            entity.blockHash = BosUtils.readBytes(buffer);
            entity.parentBlockHash = BosUtils.readBytes(buffer);
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
