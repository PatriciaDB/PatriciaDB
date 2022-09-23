package io.patriciadb.utils;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

public class Bytes {
    private final static HexFormat FORMATTER = HexFormat.of().withDelimiter(":");
    private final byte[] data;
    private int hash;

    private Bytes(byte[] data) {
        this.data = data;
    }

    public static Bytes wrap(byte[] data) {
        return new Bytes(data);
    }

    public byte[] getBytes() {
        return data.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bytes bytes = (Bytes) o;
        return hash == bytes.hash && Arrays.equals(data, bytes.data);
    }

    @Override
    public int hashCode() {
        if(hash ==  0) {
            int result = Objects.hash(hash);
            result = 31 * result + Arrays.hashCode(data);
            hash = result;
        }
        return hash;
    }

    @Override
    public String toString() {
        return "["+FORMATTER.formatHex(data)+"]";
    }
}
