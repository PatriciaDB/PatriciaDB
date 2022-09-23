package io.patriciadb.index.btree;

public class KeyRange<E extends Comparable<E>> {
    private final E fromEntry;
    private final E toEntry;

    private KeyRange(E fromEntry, E toEntry) {
        this.fromEntry = fromEntry;
        this.toEntry = toEntry;
    }

    public static <E extends Comparable<E>> KeyRange<E> of(E left, E to) {
        return new KeyRange<>(left, to);
    }

    public E getFromEntry() {
        return fromEntry;
    }

    public E getToEntry() {
        return toEntry;
    }
}
