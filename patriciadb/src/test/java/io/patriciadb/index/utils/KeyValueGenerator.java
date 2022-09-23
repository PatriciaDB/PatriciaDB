package io.patriciadb.index.utils;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public record KeyValueGenerator(long seed, int count, LengthRange keyLength, LengthRange valueLength) {


    public Stream<KeyValue> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable().iterator(), Spliterator.ORDERED), false);
    }

    public Iterable<KeyValue> iterable() {
        return () -> new Iterator<>() {
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
                byte[] key = new byte[random.nextInt(keyLength.delta()) + keyLength().minSize()];
                byte[] value = new byte[random.nextInt(keyLength.delta()) + valueLength().minSize()];
                random.nextBytes(key);
                random.nextBytes(value);
                counter++;
                return new KeyValue(key, value);
            }
        };
    }


}
