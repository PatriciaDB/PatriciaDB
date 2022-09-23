package io.patriciadb.fs.disk.utils;

public class DiskUtils {

    public static long combine(long fileId, long offset) {
        return fileId<<32 | offset;
    }
    public static int fileId(long blockPointer) {
        return (int) (blockPointer >>> 32);
    }

    public static int offset(long blockPointer) {
        return (int) (blockPointer & Integer.MAX_VALUE);
    }
}
