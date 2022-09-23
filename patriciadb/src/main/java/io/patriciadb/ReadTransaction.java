package io.patriciadb;

public interface ReadTransaction {

    StorageRead openStorage(byte[] collectionName);

}
