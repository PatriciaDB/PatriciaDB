package io.patriciadb.table;

import java.util.Optional;

public interface TableRead<E extends Entity> {

    Optional<E> get(long primaryKey);
}
