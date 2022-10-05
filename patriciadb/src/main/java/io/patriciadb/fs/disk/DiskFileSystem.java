package io.patriciadb.fs.disk;

import io.patriciadb.fs.FsReadTransaction;
import io.patriciadb.fs.FsWriteTransaction;
import io.patriciadb.fs.FileSystemError;
import io.patriciadb.fs.PatriciaFileSystem;
import io.patriciadb.fs.disk.datastorage.disk.AppenderDataStorage;
import io.patriciadb.fs.disk.transaction.TransactionManager;
import io.patriciadb.fs.disk.vacuum.VacuumCleaner;
import io.patriciadb.utils.lifecycle.BeansHolder;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class DiskFileSystem implements PatriciaFileSystem, PatriciaController {
    private final static Logger log = LoggerFactory.getLogger(DiskFileSystem.class);
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final BeansHolder beansHolder;
    private final AppenderDataStorage dataStorage;
    private final VacuumCleaner vacuumCleaner;
    private final TransactionManager transactionManager;

    public DiskFileSystem(BeansHolder beansHolder, AppenderDataStorage dataStorage,  VacuumCleaner vacuumCleaner, TransactionManager transactionManager) {
        this.beansHolder = beansHolder;
        this.dataStorage = dataStorage;
        this.vacuumCleaner = vacuumCleaner;
        this.transactionManager = transactionManager;
    }

    @Override
    public FsReadTransaction getSnapshot() {
        return transactionManager.startReadTransaction();
    }

    @Override
    public void runVacuum() {
        vacuumCleaner.fullVacuum();
    }

    @Override
    public FsWriteTransaction startTransaction() {
        return transactionManager.startWriteTransaction();
    }

    @Override
    public synchronized void close() throws FileSystemError{
        if(!isOpen.compareAndSet(true, false)) {
            return;
        }
        try {
            beansHolder.shutdown();
        } catch (FileSystemError e) {
            throw e;
        } catch (Throwable t) {
            throw new FileSystemError(true, t);
        } finally {
            isOpen.set(false);
        }
    }


}
