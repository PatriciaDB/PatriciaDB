package io.patriciadb.index.patriciamerkletrie.format;

import io.patriciadb.index.patriciamerkletrie.format.eth.EthFormat;
import io.patriciadb.index.patriciamerkletrie.format.plain.PlainFormat;

public class Formats {
    public final static Format ETHEREUM = new EthFormat();
    public final static Format PLAIN = PlainFormat.INSTANCE;

}
