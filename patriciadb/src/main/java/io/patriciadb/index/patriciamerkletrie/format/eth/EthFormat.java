package io.patriciadb.index.patriciamerkletrie.format.eth;

import io.patriciadb.index.patriciamerkletrie.format.Hasher;
import io.patriciadb.index.patriciamerkletrie.format.Format;
import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.format.StorageSerializer;
import io.patriciadb.index.patriciamerkletrie.nodes.Node;
import org.web3j.rlp.RlpEncoder;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class EthFormat implements Format {

    private final Hasher hasher = new HashImp();
    private final EthStorageSerializer storageSerializer =new EthStorageSerializer(hasher.hashLength());



    @Override
    public byte[] generateRootHash(NodeLoader nodeLoader, Node root) {
        if(root.getHash()!=null) {
            return root.getHash();
        }
        EthHashVisitor visitor = new EthHashVisitor(nodeLoader, hasher);
        var rlp=root.apply(visitor);
        var rootHash = hasher.hash(RlpEncoder.encode(rlp));
        root.setHash(rootHash);
        return rootHash;
    }

    @Override
    public boolean isNodeHashingSupported() {
        return true;
    }

    @Override
    public StorageSerializer storageSerializer() {
        return storageSerializer;
    }

    private static class HashImp implements Hasher {

        private final static int HASH_LENGTH=32;

        @Override
        public byte[] hash(byte[] value) {
            return messageDigest().digest(value);
        }

        @Override
        public MessageDigest messageDigest() {
            try {
                return MessageDigest.getInstance("Keccak-256");
            }catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int hashLength() {
            return HASH_LENGTH;
        }
    }
}
