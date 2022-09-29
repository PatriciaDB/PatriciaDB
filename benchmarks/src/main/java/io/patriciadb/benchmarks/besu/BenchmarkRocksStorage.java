package io.patriciadb.benchmarks.besu;


import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class BenchmarkRocksStorage implements KeyValueStorage {


    final RocksDB db;

    public BenchmarkRocksStorage(final  RocksDB db) {
        this.db = db;
    }

    @Override
    public void clear() throws StorageException {
    }

    @Override
    public boolean containsKey(final byte[] key) throws StorageException {
        try {
            return db.get(key) != null;
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Optional<byte[]> get(final byte[] key) throws StorageException {
        try {
            return Optional.ofNullable(db.get(key));
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Stream<byte[]> streamKeys() throws StorageException {
        return null;
    }

    @Override
    public boolean tryDelete(final byte[] key) throws StorageException {
        try {
            db.delete(key);
            return true;
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Set<byte[]> getAllKeysThat(final  Predicate<byte[]> returnCondition) {
        return Set.of();
    }

    @Override
    public KeyValueStorageTransaction startTransaction() throws StorageException {
        final WriteOptions writeOpt = new WriteOptions().setNoSlowdown(true);
        final WriteBatch batch = new WriteBatch();
        return new KeyValueStorageTransaction() {
            @Override
            public void put(final byte[] key,final  byte[] value) {
                try {
                    batch.put(key, value);
                }catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove(final byte[] key) {
                try {
                    batch.delete(key);
                }catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void commit() throws StorageException {
                try {
                    db.write(writeOpt, batch);
                }catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void rollback() {

            }
        };
    }

    @Override
    public void close() throws IOException {
        db.close();
    }
}