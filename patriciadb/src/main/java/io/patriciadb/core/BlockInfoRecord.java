package io.patriciadb.core;

import io.patriciadb.BlockInfo;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.time.Instant;
import java.util.Arrays;

public record BlockInfoRecord(byte[] blockHash, byte[] parentBlockHash, long blockNumber, Instant creationTime, String attachment, Roaring64NavigableMap newNodeIds, Roaring64NavigableMap lostNodeIds) implements BlockInfo {


    @Override
    public String toString() {
        return "BlockInfoRecord{" +
                "blockHash=" + Arrays.toString(blockHash) +
                ", parentBlockHash=" + Arrays.toString(parentBlockHash) +
                ", blockNumber=" + blockNumber +
                ", creationTime=" + creationTime +
                ", attachment='" + attachment + '\'' +
                ", newNodeIdsCardinality='" + newNodeIds.getLongCardinality() + '\'' +
                ", lostNodeIdsCardinality='" + lostNodeIds.getLongCardinality() + '\'' +
                '}';
    }
}
