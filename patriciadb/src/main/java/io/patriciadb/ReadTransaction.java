package io.patriciadb;

public interface ReadTransaction extends Releasable{

    /**
     * Open a storage by its name
     *
     * @param storeId the storeId
     * @return the readonly storage
     * @throws StorageNotFoundException if the store is not found
     */
    StorageRead openStorage(byte[] storeId);

}
