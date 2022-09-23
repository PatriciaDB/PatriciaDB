package io.patriciadb.table;

import io.patriciadb.fs.BlockWriter;

public interface Index<E extends Entity> {

     void beforeInsert(E entry);

     void insert(E entry);

     void delete(E entry);

     void update(E oldEntry, E newEntry);

     String getIndexName();

     long persist(BlockWriter blockWriter);
}
