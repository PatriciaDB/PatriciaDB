package io.patriciadb.fs.disk.directory;

public interface DirectoryTransaction extends DirectorySnapshot {

     long get(long blockid);

     void set(long blockId, long position);

     void clear(long blockId);

     long getNextFreeBlockId();

     void commit();

     void release();
}
