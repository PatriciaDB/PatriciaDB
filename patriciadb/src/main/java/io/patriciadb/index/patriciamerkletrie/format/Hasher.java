package io.patriciadb.index.patriciamerkletrie.format;

import java.security.MessageDigest;

public interface Hasher {

   byte[] hash(byte[] value);

   MessageDigest messageDigest();

   int hashLength();
}
