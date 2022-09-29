package io.patriciadb.utils.lifecycle;

public interface PatriciaController {
    default void initialize() throws Exception {

    }

    default void preDestroy() throws Exception {

    }

    default void destroy() throws Exception {

    }
}
