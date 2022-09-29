package io.patriciadb.benchmarks.utils;

public class LengthRange {
    final int minSize;
    final int maxSize;

    public LengthRange(final int minSize, final int maxSize) {
        this.minSize = minSize;
        this.maxSize = maxSize;
    }

    public int getMinSize() {
        return minSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int delta() {
        return maxSize - minSize;
    }

    public static LengthRange ofSize(final int size) {
        return new LengthRange(size, size + 1);
    }

    public static LengthRange ofRange(final int minSize, final int maxSize) {
        return new LengthRange(minSize, maxSize);
    }
}
