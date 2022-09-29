package io.patriciadb.core.utils;

import io.patriciadb.core.blocktable.BlockEntity;
import io.patriciadb.core.BlockInfoRecord;
import io.patriciadb.utils.BitMapUtils;

public class BlockEntityConverter {


    public static BlockInfoRecord fromBlockEntity(BlockEntity entity) {
        return new BlockInfoRecord(entity.getBlockHash().clone(),
                entity.getParentBlockHash().clone(),
                entity.getBlockNumber(),
                entity.getCreationTime(),
                entity.getExtra().clone(),
                BitMapUtils.deserialize(entity.getNewNodeIds()),
                BitMapUtils.deserialize(entity.getLostNodeIds()));
    }
}
