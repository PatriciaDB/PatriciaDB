package io.patriciadb.index.patriciamerkletrie.utils;

import io.patriciadb.core.transactionstable.TransactionEntity;
import io.patriciadb.utils.BitMapUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public record TrieDeltaChanges(Roaring64NavigableMap newNodes, Roaring64NavigableMap lostNodes) {


    public static TrieDeltaChanges fromBytes(byte[] newNodesBytes, byte[] lostNodesBytes) {
        var newNodeIds = BitMapUtils.deserialize(newNodesBytes);
        var lostNodeIds = BitMapUtils.deserialize(lostNodesBytes);
        return new TrieDeltaChanges(newNodeIds, lostNodeIds);
    }

    public static TrieDeltaChanges fromBlockEntity(TransactionEntity block) {
        return fromBytes(block.getNewNodeIds(), block.getLostNodeIds());
    }

    public void setToBlock(TransactionEntity entity) {
        entity.setNewNodeIds(BitMapUtils.serialize(newNodes));
        entity.setLostNodeIds(BitMapUtils.serialize(lostNodes));
    }


}
