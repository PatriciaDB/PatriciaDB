package io.patriciadb.table;

import io.patriciadb.utils.Serializer;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMetadata {
    public static final Serializer<TableMetadata> SERIALIZER = new TableMetadataSerializer();
    private final Map<String, Long> indexRoots;

    public TableMetadata(Map<String, Long> indexRoots) {
        this.indexRoots = indexRoots;
    }

    public TableMetadata() {
        this.indexRoots = new HashMap<>();
    }

    public long getPrimaryKey() {
        return indexRoots.getOrDefault("primaryKey", 0L);
    }

    public void setPrimaryKey(long primaryKey) {
        indexRoots.put("primaryKey", primaryKey);
    }

    public void setSecondaryIndexNodeId(String indexName, long rootId) {
        indexRoots.put(indexName, rootId);
    }

    public List<String> getSecondaryIndexName() {
        return indexRoots.keySet().stream()
                .filter(e -> !e.equalsIgnoreCase("primaryKey"))
                .toList();
    }
    public long getSecondaryIndexRootNodeId(String indexName) {
        return indexRoots.getOrDefault(indexName, 0L);
    }


    private static class TableMetadataSerializer implements Serializer<TableMetadata> {
        @Override
        public void serialize(TableMetadata entry, ByteArrayOutputStream bos) {
            try {
                DataOutputStream dos = new DataOutputStream(bos);
                dos.writeInt(entry.indexRoots.size());
                for (var e : entry.indexRoots.entrySet()) {
                    dos.writeUTF(e.getKey());
                    dos.writeLong(e.getValue());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public TableMetadata deserialize(ByteBuffer buffer) {
            int remaining = buffer.remaining();
            byte[] data = new byte[remaining];
            buffer.get(data);
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            try {
                int count = dis.readInt();
                HashMap<String, Long> result = new HashMap<>();
                for(int i=0; i< count; i++) {
                    String key = dis.readUTF();
                    long nodeId = dis.readLong();
                    result.put(key,nodeId);
                }
                return new TableMetadata(result);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
