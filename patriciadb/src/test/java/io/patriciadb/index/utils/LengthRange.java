package io.patriciadb.index.utils;

public record LengthRange(int minSize, int maxSize) {

    public LengthRange {
        assert minSize >= 0;
        assert minSize < maxSize;
    }

    public int delta() {
        return maxSize - minSize;
    }

    public static LengthRange ofSize(int size) {
        return new LengthRange(size, size + 1);
    }

    public static LengthRange ofRange(int minSize, int maxSize) {
        return new LengthRange(minSize, maxSize);
    }
}
