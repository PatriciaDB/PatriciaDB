package io.patriciadb.fs.disk;

import io.patriciadb.fs.disk.datastorage.disk.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataStorageFactory {


    public static AppenderDataStorage openDirectory(Path directory, int maxAppenderSize) throws IOException {
        if (!Files.exists(directory)) {
            throw new StorageOpenException(directory + " does not exists");
        }
        if (!Files.isDirectory(directory)) {
            throw new StorageOpenException(directory + " is not a directory");
        }
        if (!Files.isWritable(directory)) {
            throw new StorageOpenException(directory + " is not writable");
        }
        if (maxAppenderSize <= 50 * 1024 * 1024) {
            throw new IllegalArgumentException("Invalid maxAppenderSize (min is 50MB)");
        }
        var dataFilePaths = Files.list(directory)
                .filter(f -> f.toString().endsWith(".data"))
                .toList();
        HashMap<Integer, FileDataChannel> channels = new HashMap<>();
        for (var dataFile : dataFilePaths) {
            var dataFileChannel = FileDataChannel.open(dataFile, FileDataChannel.OpenMode.READ_WRITE);
            if (channels.containsKey(dataFileChannel.getFileId())) {
                throw new StorageOpenException("Duplicate FileDataId found");
            }
            channels.put(dataFileChannel.getFileId(), dataFileChannel);
        }
        if (channels.isEmpty()) {
            var fileAppenderFactory = new FileAppenderFactoryImp(directory, maxAppenderSize, 1);
            var currentAppender = fileAppenderFactory.newFileDataAppender();
            return new AppenderDataStorage(List.of(), currentAppender, fileAppenderFactory);
        } else {
            var maxId = channels.keySet().stream().mapToInt(Integer::intValue).max().getAsInt();
            var maxIdChannel = channels.remove(maxId);
            List<FileReader> readers = new ArrayList<>();
            for (var readerChannel : channels.values()) {
                readers.add(new FileDataMMapReader(readerChannel));
            }
            var appender = new FileDataAppender(maxIdChannel, FileDataAppender.DEFAULT_BUFFER_SIZE, FileDataAppender.MMAP_DEFAULT_BLOCK_SIZE, maxAppenderSize);
            var fileAppenderFactory = new FileAppenderFactoryImp(directory, maxAppenderSize, 1);
            return new AppenderDataStorage(readers, appender, fileAppenderFactory);
        }
    }
}
