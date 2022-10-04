package io.patriciadb.fs.disk.vacuum;

import io.patriciadb.fs.TaskScheduledExecutor;
import io.patriciadb.fs.disk.datastorage.DataStorage;
import io.patriciadb.fs.disk.datastorage.disk.AppenderDataStorage;
import io.patriciadb.fs.disk.directory.Directory;
import io.patriciadb.fs.disk.directory.imp.DiskMMapDirectory;
import io.patriciadb.fs.disk.directory.imp.WriteAheadLogDirectory;
import io.patriciadb.fs.disk.utils.DiskUtils;
import io.patriciadb.fs.disk.utils.LongLongPair;
import io.patriciadb.utils.lifecycle.PatriciaController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

public class VacuumCleaner implements PatriciaController {
    private final static Logger log = LoggerFactory.getLogger(VacuumCleaner.class);
    private final WriteAheadLogDirectory writeAheadLogDirectory;
    private final DiskMMapDirectory directory;
    private final AppenderDataStorage storage;
    private final TaskScheduledExecutor executor;

    public VacuumCleaner(WriteAheadLogDirectory writeAheadLogDirectory,
                         DiskMMapDirectory directory,
                         AppenderDataStorage storage,
                         TaskScheduledExecutor executor) {
        this.writeAheadLogDirectory = writeAheadLogDirectory;
        this.directory = directory;
        this.storage = storage;
        this.executor = executor;
    }

    public void fullVacuum() {
        log.info("VacuumFull started");
        var fileDataList = storage.getFileDataList().stream().sorted(Comparator.comparingLong(l -> l.getHeader().sequenceNumber())).toList();
        storage.rollAppender();
        writeAheadLogDirectory.syncFully();
        for (var fileData : fileDataList) {
            if (!fileData.isReadOnly()) {
                continue;
            }
            int fileId = fileData.getFileId();
            log.debug("Vacuuming file id {}", fileId);
            var blockForFile = directory.get((blockId, pointer) -> DiskUtils.fileId(pointer) == fileId);
            blockForFile.sort(Comparator.comparingLong(LongLongPair::right));
            long counter = 0;
            long byteCounts = 0;
            long startTime = System.currentTimeMillis();
            for (var block : blockForFile) {
                counter++;
                var buffer = storage.read(block.right());
                byteCounts += buffer.remaining();
                var newPointer = storage.write(buffer);
                directory.compareAndSet(block.left(), block.right(), newPointer);
            }
            log.debug("Copied {} blocks and {} bytes in {}ms", counter, byteCounts, System.currentTimeMillis() - startTime);
            storage.flushAndSync();
            directory.sync();
            storage.delete(fileId);
            log.debug("Deleting file data with id {}", fileId);
        }

        log.info("VacuumFull completed");

    }
}
