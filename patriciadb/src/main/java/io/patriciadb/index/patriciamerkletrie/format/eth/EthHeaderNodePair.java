package io.patriciadb.index.patriciamerkletrie.format.eth;

import io.patriciadb.index.patriciamerkletrie.format.HeaderNodePair;

public interface EthHeaderNodePair extends HeaderNodePair {

    EthNodeHeader getHeader();
}
