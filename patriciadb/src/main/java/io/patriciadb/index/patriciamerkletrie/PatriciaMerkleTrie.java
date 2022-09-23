package io.patriciadb.index.patriciamerkletrie;

import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.visitors.FindVisitor;
import io.patriciadb.index.patriciamerkletrie.visitors.NodePrinterVisitor;
import io.patriciadb.fs.BlockReader;
import io.patriciadb.fs.BlockWriter;
import io.patriciadb.index.patriciamerkletrie.format.Format;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.Node;
import io.patriciadb.index.patriciamerkletrie.serializer.SerializerPersisterVisitor;
import io.patriciadb.index.patriciamerkletrie.utils.Nibble;
import io.patriciadb.index.patriciamerkletrie.visitors.DeleteVisitor;
import io.patriciadb.index.patriciamerkletrie.visitors.InsertionVisitor;

public class PatriciaMerkleTrie {
    private final NodeLoader nodeLoader;
    private final Format format;
    private final PersistedNodeObserver persistedNodeObserver;
    private Node root;

    private PatriciaMerkleTrie(NodeLoader nodeLoader, Format format, Node root, PersistedNodeObserver persistedNodeObserver) {
        this.nodeLoader = nodeLoader;
        this.format = format;
        this.persistedNodeObserver = persistedNodeObserver;
        this.root = root;
    }

    public static PatriciaMerkleTrie open(Format format, long rootId, BlockReader reader) {
        return open(format, rootId, reader, null);
    }

    public static PatriciaMerkleTrie open(Format format, long rootId, BlockReader reader, PersistedNodeObserver persistedNodeObserver) {
        NodeLoader nodeLoader = new NodeLoader(reader, format.storageSerializer());
        var root = nodeLoader.loadFromStorage(rootId).getNode();
        return new PatriciaMerkleTrie(nodeLoader, format, root, persistedNodeObserver);
    }

    public static PatriciaMerkleTrie openOrCreate(Format format, long rootId, BlockReader reader, PersistedNodeObserver persistedNodeObserver) {
        if(rootId == 0) {
            return createNew(format, persistedNodeObserver);
        } else {
            return open(format, rootId, reader, persistedNodeObserver);
        }
    }

    public static PatriciaMerkleTrie openOrCreate(Format format, long rootId, BlockReader reader) {
        return openOrCreate(format, rootId, reader, null);
    }

    public static PatriciaMerkleTrie createNew(Format format) {
        return createNew(format, null);
    }

    public static PatriciaMerkleTrie createNew(Format format, PersistedNodeObserver persistedNodeObserver) {
        NodeLoader nodeLoader = new NodeLoader(id -> null, format.storageSerializer());
        return new PatriciaMerkleTrie(nodeLoader, format, EmptyNode.INSTANCE, persistedNodeObserver);
    }

    public void printNodes() {
        var visitor = new NodePrinterVisitor(nodeLoader, " ");
        root.apply(visitor, 0);
    }

    public boolean put(byte[] key, byte[] value) {
        var visitor = new InsertionVisitor(nodeLoader, value.clone(), persistedNodeObserver);
        var result = root.apply(visitor, Nibble.forKey(key));
        if (result.subtreeChanged()) {
            root = result.subChild();
        }
        return result.subtreeChanged();
    }

    public boolean delete(byte[] key) {
        var result = DeleteVisitor.delete(nodeLoader, root, Nibble.forKey(key), persistedNodeObserver);
        if (!result.modified()) {
            return false;
        }
        root = result.newRootNode();
        return true;
    }

    public byte[] get(byte[] key) {
        var visitor = new FindVisitor(nodeLoader);
        return root.apply(visitor, Nibble.forKey(key));
    }

    public byte[] getRootHash() {
        return format.generateRootHash(nodeLoader, root);
    }

    public long persist(BlockWriter writer) {
        return SerializerPersisterVisitor.persist(writer, format.storageSerializer(), root, persistedNodeObserver);
    }

}
