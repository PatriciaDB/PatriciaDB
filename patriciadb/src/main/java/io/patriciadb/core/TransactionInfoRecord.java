package io.patriciadb.core;

import io.patriciadb.TransactionInfo;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.time.Instant;
import java.util.Arrays;

public record TransactionInfoRecord(byte[] transactionId,
                                    byte[] parentTransactionId,
                                    long blockNumber,
                                    Instant creationTime,
                                    byte[] attachment,
                                    Roaring64NavigableMap newNodeIds,
                                    Roaring64NavigableMap lostNodeIds)
        implements TransactionInfo {


    @Override
    public String toString() {
        return "TransactionInfoRecord{" +
                "transactionId=" + Arrays.toString(transactionId) +
                ", parentTransactionId=" + Arrays.toString(parentTransactionId) +
                ", blockNumber=" + blockNumber +
                ", creationTime=" + creationTime +
                ", attachment='" + attachment.length + '\'' +
                ", newNodeIdsCardinality='" + newNodeIds.getLongCardinality() + '\'' +
                ", lostNodeIdsCardinality='" + lostNodeIds.getLongCardinality() + '\'' +
                '}';
    }
}
