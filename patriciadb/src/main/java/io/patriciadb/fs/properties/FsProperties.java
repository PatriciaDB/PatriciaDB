package io.patriciadb.fs.properties;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class FsProperties {
    private final Map<String,String> props;

    public FsProperties(Map<String, String> props) {
        this.props = Map.copyOf(props);
    }

    public String get(String key) {
        return props.get(key);
    }

    public Optional<String> getAsOptional(String key) {
        if(props.containsKey(key)) {
            return Optional.of(get(key));
        } else {
            return Optional.empty();
        }
    }

    private String getAsString(String key, String defaultVal) {
        return props.getOrDefault(key, defaultVal);
    }

    public FileSystemType getFileSystemType() {
        return FileSystemType.valueOf(getAsString(PropertyConstants.FS_TYPE, FileSystemType.IN_MEMORY.name()));
    }

    public Path getDataFolder() {
        var path = getAsOptional(PropertyConstants.FS_DATA_FOLDER).orElseThrow(() -> new IllegalArgumentException("Data directory missing"));
        return Path.of(path);
    }

    public OptionalLong maxWalFileSystem() {
        return getAsOptional(PropertyConstants.FS_MAX_WALL_FILE_SIZE)
                .map(Long::parseLong)
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());
    }

    public OptionalInt maxFileDataFileSize() {
        return getAsOptional(PropertyConstants.FS_MAX_DATA_FILE_SIZE)
                .map(Integer::parseInt)
                .map(OptionalInt::of)
                .orElse(OptionalInt.empty());
    }

}
