package io.patriciadb.index.patriciamerkletrie.format.eth;

import io.patriciadb.index.patriciamerkletrie.format.Header;

import java.util.Optional;

public interface EthNodeHeader extends Header {

    Optional<byte[]> getHash();


}
