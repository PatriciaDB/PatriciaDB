package io.patriciadb.benchmarks.utils;


import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KeyValueGenerator {
   final long seed;
    final int count;
    final LengthRange keyLength;
    final LengthRange valueLength;

    public KeyValueGenerator(final long seed, final int count, final LengthRange keyLength, final LengthRange valueLength) {
        this.seed = seed;
        this.count = count;
        this.keyLength = keyLength;
        this.valueLength = valueLength;
    }

    public Stream<KeyValue> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable().iterator(), Spliterator.ORDERED), false);
    }

    public Iterable<KeyValue> iterable() {
        return () -> new Iterator<KeyValue>() {
            final Random random = new Random(seed);
            int counter = 0;

            @Override
            public boolean hasNext() {
                return counter < count;
            }

            @Override
            public KeyValue next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                byte[] key = new byte[random.nextInt(keyLength.delta()) + keyLength.getMinSize()];
                byte[] value = new byte[random.nextInt(keyLength.delta()) + valueLength.getMinSize()];
                random.nextBytes(key);
                random.nextBytes(value);
                counter++;
                return new KeyValue(key, value);
            }
        };
    }


}
