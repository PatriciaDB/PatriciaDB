package io.patriciadb.index.utils;

import java.util.Arrays;

public record KeyValue(byte[] key, byte[] value) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValue keyValue = (KeyValue) o;
        return Arrays.equals(key, keyValue.key) && Arrays.equals(value, keyValue.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
