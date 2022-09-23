package io.patriciadb.core.utils;

import io.patriciadb.core.blocktable.BlockEntity;
import io.patriciadb.core.BlockInfoRecord;
import io.patriciadb.utils.BitMapUtils;

public class BlockEntityConverter {


    public static BlockInfoRecord fromBlockEntity(BlockEntity entity) {
        return new BlockInfoRecord(entity.getBlockHash(),
                entity.getParentBlockHash(),
                entity.getBlockNumber(),
                entity.getCreationTime(),
                entity.getExtra(),
                BitMapUtils.deserialize(entity.getNewNodeIds()),
                BitMapUtils.deserialize(entity.getLostNodeIds()));
    }
}
