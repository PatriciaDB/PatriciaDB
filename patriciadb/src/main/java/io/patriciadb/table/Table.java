package io.patriciadb.table;

public interface Table<E extends Entity> extends TableRead<E> {

    void insert(E entity);

    void update(E entity);

    void delete(long primaryKey);

    void persist();
}
