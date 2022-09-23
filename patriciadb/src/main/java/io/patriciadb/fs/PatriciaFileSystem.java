package io.patriciadb.fs;

import io.patriciadb.utils.ExceptionUtils;

import java.util.function.Function;

public interface PatriciaFileSystem {

    FSSnapshot getSnapshot();

    FSTransaction startTransaction();

    void sync();

    default <T> T startTransaction(Function<FSTransaction, T> runnable) {
        var tr = startTransaction();
        try {
            var res= runnable.apply(tr);
            tr.commit();;
            return res;
        }catch (Throwable t) {
            throw ExceptionUtils.sneakyThrow(t);
        } finally {
            tr.release();
        }
    }

    void close();


    default <T> T getSnapshot(Function<FSSnapshot, T> runnable) {
        var tr = getSnapshot();
        try {
            return runnable.apply(tr);
        }catch (Throwable t) {
            throw ExceptionUtils.sneakyThrow(t);
        } finally {
            tr.release();
        }
    }
}
