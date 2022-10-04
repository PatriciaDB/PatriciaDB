package io.patriciadb.core.utils;

import io.patriciadb.core.transactionstable.TransactionEntity;
import io.patriciadb.core.TransactionInfoRecord;
import io.patriciadb.utils.BitMapUtils;

public class TransactionInfoMapper {


    public static TransactionInfoRecord fromBlockEntity(TransactionEntity entity) {
        return new TransactionInfoRecord(entity.getTransactionId().clone(),
                entity.getParentTransactionId().clone(),
                entity.getBlockNumber(),
                entity.getCreationTime(),
                entity.getExtra().clone(),
                BitMapUtils.deserialize(entity.getNewNodeIds()),
                BitMapUtils.deserialize(entity.getLostNodeIds()));
    }
}
