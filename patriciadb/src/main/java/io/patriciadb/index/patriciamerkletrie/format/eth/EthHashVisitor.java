package io.patriciadb.index.patriciamerkletrie.format.eth;

import io.patriciadb.index.patriciamerkletrie.format.Hasher;
import io.patriciadb.index.patriciamerkletrie.io.NodeLoader;
import io.patriciadb.index.patriciamerkletrie.nodes.BranchNode;
import io.patriciadb.index.patriciamerkletrie.nodes.EmptyNode;
import io.patriciadb.index.patriciamerkletrie.nodes.ExtensionNode;
import io.patriciadb.index.patriciamerkletrie.nodes.LeafNode;
import io.patriciadb.index.patriciamerkletrie.visitors.ResultVisitor;
import org.web3j.rlp.RlpEncoder;
import org.web3j.rlp.RlpList;
import org.web3j.rlp.RlpString;
import org.web3j.rlp.RlpType;

import java.util.ArrayList;

public class EthHashVisitor implements ResultVisitor<RlpType> {

    private final NodeLoader nodeLoader;
    private final Hasher hash;
    private final RlpType NULL_STRING=RlpString.create(new byte[0]);

    public EthHashVisitor(NodeLoader nodeLoader, Hasher hash) {
        this.nodeLoader = nodeLoader;
        this.hash = hash;
    }

    @Override
    public RlpType apply(BranchNode branch) {
        ArrayList<RlpType> childEncoded = new ArrayList<>();
        for(int i=0; i<16; i++) {
            if(branch.getChild(i)==null) {
                childEncoded.add(NULL_STRING);
                continue;
            }
            var childNodePair =(EthHeaderNodePair) nodeLoader.loadHashNodePair(branch.getChild(i));
            if(childNodePair.getHeader().getHash().isPresent()) {
                childEncoded.add(RlpString.create(childNodePair.getHeader().getHash().get()));
                continue;
            }

            var childNode = childNodePair.getNode();
            var childRlp = childNode.apply(this);
            var nodeBytes = RlpEncoder.encode(childRlp);
            if(nodeBytes.length<32) {
                childEncoded.add(childRlp);
            } else {
                var childHash = hash.hash(nodeBytes);
                childNode.setHash(childHash);
                childEncoded.add(RlpString.create(childHash));
            }
        }
        if(branch.getValue()==null) {
            childEncoded.add(NULL_STRING);
        } else {
            var valueNode=(LeafNode) nodeLoader.loadNode(branch.getValue());
            childEncoded.add(RlpString.create(valueNode.getValue()));
        }

        return new RlpList(childEncoded);
    }

    @Override
    public RlpType apply(ExtensionNode extension) {
        var nextChildPair = (EthHeaderNodePair)nodeLoader.loadHashNodePair(extension.getNextNode());
        var header = nextChildPair.getHeader();
        var nibble = extension.getNibble();
        if(header.getHash().isPresent()) { // NextChild not a leaf if it has the Hash
            var k = RlpString.create(nibble.isOdd() ? nibble.packWithPrefix(1) : nibble.packWithPrefix(0));
            return new RlpList(k, RlpString.create(header.getHash().get()));
        }
        if(nextChildPair.getNode() instanceof LeafNode leafNode) {
            var k = RlpString.create(nibble.isOdd() ? nibble.packWithPrefix(3) : nibble.packWithPrefix(2));
            var v = RlpString.create(leafNode.getValue());
            return new RlpList(k, v);
        }
        var k = RlpString.create(nibble.isOdd() ? nibble.packWithPrefix(1) : nibble.packWithPrefix(0));

        var nextChild = nextChildPair.getNode();
        var nextChildRlp = nextChild.apply(this);
        var encodedChild = RlpEncoder.encode(nextChildRlp);
        RlpType l;
        if(encodedChild.length<32) {
            l=nextChildRlp;
        } else {
            var childHash = hash.hash(encodedChild);
            nextChild.setHash(childHash);
            l=RlpString.create(childHash);
        }
        return new RlpList(k, l);
    }

    @Override
    public RlpType apply(LeafNode leaf) {
        var k = RlpString.create(new byte[]{0x20});
        var v = RlpString.create(leaf.getValue());
        return new RlpList(k, v);
    }

    @Override
    public RlpType apply(EmptyNode empty) {
        return RlpString.create(new byte[0]);
    }
}
