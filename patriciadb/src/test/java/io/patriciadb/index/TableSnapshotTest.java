package io.patriciadb.index;

import io.patriciadb.core.blocktable.BlockEntity;
import io.patriciadb.core.blocktable.BlockTableFactory;
import io.patriciadb.fs.disk.DiskFileSystem;
import io.patriciadb.table.Tables;

import java.io.File;
import java.time.Instant;

public class TableSnapshotTest {

    public static void main(String[] args) throws Exception {
        var fs = new DiskFileSystem(new File("/Users/dguiducci/temp/db").toPath(), 60*1024*1024);
        {
            var transaction = fs.startTransaction();
            try {
                Tables.createNew(BlockTableFactory.INSTANCE, transaction, 100);
                transaction.commit();
            } finally {
                transaction.release();
            }
        }

        for(int batch=0; batch<1000; batch++) {
            var transaction = fs.startTransaction();
            try {
                var table = Tables.open(BlockTableFactory.INSTANCE, transaction, 100);
                for(int i=0; i< 1000; i++) {
                    var s1 = new BlockEntity();
                    s1.setBlockHash(("snapshotId"+batch+"-"+i).getBytes());
                    s1.setParentBlockHash("parentId".getBytes());
                    s1.setBlockNumber(batch*10000+i);
                    s1.setCreationTime(Instant.now());
                    s1.setExtra("extra1");
                    s1.setIndexRootNodeId(333);
                    table.insert(s1);
                }
//                var elem = table.findBySnapshotId("snapshotId1".getBytes());

                table.persist();

                transaction.commit();

            } finally {
                transaction.release();
            }
        }
        System.out.println("Inserted 1M elements");
//        System.out.println(fs.getBlockCount());
//        System.out.println(fs.getDataSize());

        Thread.sleep(30000);
        for(int batch=0; batch<1000; batch++) {
            var transaction = fs.startTransaction();
            try {
                var table = Tables.open(BlockTableFactory.INSTANCE, transaction, 100);
                for(int i=0; i< 1000; i++) {
                    var elem =table.findByBlockHash(("snapshotId"+batch+"-"+i).getBytes());
                    table.delete(elem.get().getPrimaryKey());
                }
//                var elem = table.findBySnapshotId("snapshotId1".getBytes());

                table.persist();

                transaction.commit();

            } finally {
                transaction.release();
            }
        }
        System.out.println("Removed 1M elements");
//        System.out.println(fs.getBlockCount());
//        System.out.println(fs.getDataSize());

//        var tr2= fs.getSnapshot();
//        try {
//            var table = Tables.openReadOnly(SnapshotTableFactory.INSTANCE, tr2,100);
//
//            var elem =table.findBySnapshotId("snapshotId100-45".getBytes());
//
//            System.out.println(elem.orElse(null));
//
//        } finally {
//            tr2.release();
//        }
//
//        System.out.println(fs.getBlockCount());
//        System.out.println(fs.getDataSize());
    }
}
