package io.patriciadb.utils;

public enum Space {

    BYTE(1L),
    KILOBYTE(1L<<10),
    MEGABYTE(1L<<20),
    GIGABYTE(1L<<30),
    TERABYTE(1L<<40),
    PETABYTE(1L<<50);

    private final long oneToBytes;

    Space(long oneToBytes) {
        this.oneToBytes = oneToBytes;
    }

    public long toBytes(long value) {
        return value*oneToBytes;
    }
}
