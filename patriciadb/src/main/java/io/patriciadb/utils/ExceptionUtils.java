package io.patriciadb.utils;

public class ExceptionUtils {

    public static <E extends Throwable> E sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

}
