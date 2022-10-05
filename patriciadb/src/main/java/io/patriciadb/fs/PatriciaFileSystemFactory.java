package io.patriciadb.fs;

import io.patriciadb.fs.disk.*;
import io.patriciadb.fs.disk.directory.imp.*;
import io.patriciadb.fs.disk.transaction.TransactionManager;
import io.patriciadb.fs.disk.vacuum.VacuumCleaner;
import io.patriciadb.fs.properties.FileSystemType;
import io.patriciadb.fs.properties.FsProperties;
import io.patriciadb.fs.simple.SimpleFileSystem;
import io.patriciadb.utils.finalizer.Finalizer;
import io.patriciadb.utils.lifecycle.BeansHolder;

import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PatriciaFileSystemFactory {

    public static PatriciaFileSystem inMemoryFileSystem() {
        return new SimpleFileSystem();
    }

    public static PatriciaFileSystem createFromProperties(Map<String, String> properties) {
        try {
            var prop = new FsProperties(properties);

            var fsType = prop.getFileSystemType();
            if (fsType == FileSystemType.IN_MEMORY) {
                return new SimpleFileSystem();
            } else if (fsType == FileSystemType.APPENDER) {
                return createAppender(prop);
            } else {
                throw new IllegalArgumentException("FileSystem " + fsType + " unknown");
            }
        } catch (Exception ex) {
            throw new StorageOpenException(ex);
        }

    }

    private static PatriciaFileSystem createAppender(FsProperties fsProperties) throws Exception {
        var dataDirectory = fsProperties.getDataFolder();
        if (!Files.exists(dataDirectory)) {
            throw new IllegalArgumentException("Data directory " + dataDirectory + " does not exist");
        }
        if (!Files.isDirectory(dataDirectory)) {
            throw new IllegalArgumentException("Data directory path is not a folder " + dataDirectory);
        }
        if (!Files.isWritable(dataDirectory)) {
            throw new IllegalArgumentException("Data directory is not writable");
        }
        BeansHolder beanHolder = new BeansHolder();
        var walDirFile = dataDirectory.resolve("directory.log");

        DiskMMapDirectory diskMMapDirectory = beanHolder.addBean(() -> new DiskMMapDirectory(dataDirectory.resolve("directory")));

        var executorService = beanHolder.addBean(() -> new TaskScheduledExecutor(new ScheduledThreadPoolExecutor(1, new FsThreadFactory())));
//        var callbackExecutor = beanHolder.addBean(() -> new CallbackExecutor(new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>())));

        var maxWalFileSize = fsProperties.maxWalFileSystem().orElse(WriteAheadLogDirectory.DEFAULT_MAX_WAL_LOG_FILE_SIZE);
        var walDirectory = beanHolder.addBean(() -> new WriteAheadLogDirectory(diskMMapDirectory, walDirFile, maxWalFileSize));

        var dataStorage = beanHolder.addBean(() -> DataStorageFactory.openDirectory(dataDirectory, fsProperties));

        var finalizer = beanHolder.addBean(Finalizer::new);
        var transactionManager = beanHolder.addBean(() -> new TransactionManager(dataStorage, walDirectory, finalizer));
        var vacuumCleaner = beanHolder.addBean(() -> new VacuumCleaner(walDirectory, diskMMapDirectory, dataStorage, executorService));
        var diskFileSystem = beanHolder.addBean(() -> new DiskFileSystem(beanHolder, dataStorage, vacuumCleaner,transactionManager));
        beanHolder.start();
        return diskFileSystem;
    }


}
