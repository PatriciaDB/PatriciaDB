package io.patriciadb.index.patriciamerkletrie.utils;



import io.patriciadb.utils.VarInt;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;


/**
 * A Nibble represents a piece of a (binary) key expanded to an hex-decimal string.
 */
public class Nibble implements Comparable<Nibble> {
    public final static Nibble EMPTY = new Nibble(new byte[0]);

    private final byte[] expandedKey;

    private Nibble(byte[] expandedKey) {
        this.expandedKey = expandedKey;
    }

    /**
     * Get the value of this nibble at position <i>index</i>.
     * <p>
     * Because a nibble is an hexadecimal representations, this method returns a number from 0 to 15 (inclusive).
     * <p>
     * The index must be less than {@link Nibble#size()}
     *
     * @param index the index to read
     * @return
     */
    public byte get(int index) {
        return expandedKey[index];
    }

    /**
     * @return the length of this nibble. Can be zero, but never negative
     */
    public int size() {
        return expandedKey.length;
    }

    /**
     * @return true if the length of this Nibble is odd
     */
    public boolean isOdd() {
        return expandedKey.length % 2 != 0;
    }

    /**
     * @return true if the length of this nibble is even
     */
    public boolean isEven() {
        return expandedKey.length % 2 == 0;
    }

    /**
     * @return true if the size of this nubble is zero
     */
    public boolean isEmpty() {
        return expandedKey.length == 0;
    }

    /**
     * @param prefix the prefix to verify
     * @return true if this Nibble starts with the prefix
     */
    public boolean startWith(Nibble prefix) {
        if (prefix.isEmpty()) {
            return true;
        }
        int prefixLength = prefix.expandedKey.length;
        if (expandedKey.length < prefixLength) {
            return false;
        }
        return Arrays.equals(expandedKey, 0, prefixLength, prefix.expandedKey, 0, prefixLength);
    }

    /**
     * Discard the first N characters of this nibble.
     *
     * @param sizeToDiscard number of characters to discard from this nibble
     * @return a new nibble with the prefix removed
     */
    public Nibble removePrefix(int sizeToDiscard) {
        return slice(sizeToDiscard, expandedKey.length);
    }

    /**
     * @param nibble
     * @return if the content of the nibbles are identical
     */
    public boolean equals(Nibble nibble) {
        return Arrays.equals(expandedKey, nibble.expandedKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Nibble nibble = (Nibble) o;
        return Arrays.equals(expandedKey, nibble.expandedKey);
    }

    public int hashCode() {
        return Arrays.hashCode(expandedKey);
    }

    /**
     * Creates a new nibble, concatenating this nibble with another one
     *
     * @param nibble the nibble you want to concatenate with
     * @return the newly created nibble
     */
    public Nibble append(Nibble nibble) {
        byte[] newKey = Arrays.copyOf(expandedKey, expandedKey.length + nibble.expandedKey.length);
        System.arraycopy(nibble.expandedKey, 0, newKey, expandedKey.length, nibble.expandedKey.length);
        return new Nibble(newKey);
    }

    /**
     * Append one character at the end of this Nibble and returns the modified one.
     * The value of the character to append can only be in the range from 0 to 15.
     *
     * @param chunk a chunk
     * @return the new Nibble with the chunk included at the end
     * @throws IllegalArgumentException if chunk is not in the range between 0-15 (inclusive)
     */
    public Nibble append(int chunk) {
        if (chunk < 0 || chunk > 15) {
            throw new IllegalArgumentException("Chunk must be between 0 and 15");
        }
        byte[] newKey = Arrays.copyOf(expandedKey, expandedKey.length + 1);
        newKey[newKey.length - 1] = (byte) chunk;
        return new Nibble(newKey);
    }

    /**
     * Deserialize a Nibble from this ByteBuffer.
     *
     * @param b the source ByteBuffer
     * @return the deserialised Nibble
     */
    public static Nibble deserializeWithHeader(ByteBuffer b) {
        int header = VarInt.getVarInt(b);
        boolean isOdd = (header & 0x01) == 1;
        int size = header >>> 1;
        if (size == 0) {
            return EMPTY;
        }
        byte[] nibble = new byte[size];
        b.get(nibble);
        return Nibble.forKey(nibble, isOdd);
    }

    public int estimateSizeSerializedWithHeader() {
        if (expandedKey.length == 0) {
            return 1;
        }
        int length = expandedKey.length;
        int dataSize = (length % 2 == 0) ? (length / 2) : (length / 2 + 1);
        int header = dataSize << 2 | (isOdd() ? 1 : 0);
        int headerSize = VarInt.varLongSize(header);
        return dataSize + headerSize;
    }

    /**
     * Serialize this Nibble to the target byteBuffer. Be sure that there is enough space.
     *
     * @param dest the destination ByteBuffer
     */
    public void serializeWithHeader(ByteBuffer dest) {
        if (expandedKey.length == 0) {
            dest.put((byte) 0);
            return;
        }
        int length = expandedKey.length;
        int dataSize = (length % 2 == 0) ? (length / 2) : (length / 2 + 1);
        int header = dataSize << 1 | (isOdd() ? 1 : 0);
        VarInt.putVarLong(header, dest);
        dest.put(pack());
    }

    public void serializeWithHeader(OutputStream dest) throws IOException {
        if (expandedKey.length == 0) {
            dest.write(0);
            return;
        }
        int length = expandedKey.length;
        int dataSize = (length % 2 == 0) ? (length / 2) : (length / 2 + 1);
        int header = dataSize << 1 | (isOdd() ? 1 : 0);
        VarInt.putVarLong(header, dest);
        dest.write(pack());
    }

    public void serializeWithHeader(ByteArrayOutputStream bos) {
        if (expandedKey.length == 0) {
            bos.write(0);
            return;
        }
        int length = expandedKey.length;
        int dataSize = (length % 2 == 0) ? (length / 2) : (length / 2 + 1);
        int header = dataSize << 1 | (isOdd() ? 1 : 0);
        VarInt.putVarLong(header, bos);
        bos.writeBytes(pack());
    }

    public byte[] serializeWithHeader() {
        if (expandedKey.length == 0) {
            return new byte[]{0};
        }
        int length = expandedKey.length;
        int dataSize = (length % 2 == 0) ? (length / 2) : (length / 2 + 1);
        int header = dataSize << 1 | (isOdd() ? 1 : 0);
        int headerSize = VarInt.varLongSize(header);
        ByteBuffer packed = ByteBuffer.allocate(dataSize + headerSize);
        VarInt.putVarLong(header, packed);
        packed.put(pack());
        return packed.array();
    }

    /**
     * Returns a binary, compact representations of this Nibble.
     *
     * @return the binary compact format of this Nibble without headers
     */
    public byte[] pack() {
        if (expandedKey.length == 0) {
            return new byte[0];
        }
        int length = expandedKey.length;
        byte[] packed = new byte[(length % 2 == 0) ? (length / 2) : (length / 2 + 1)];
        for (int i = 0; i < length; i++) {
            packed[i / 2] |= i % 2 == 0 ? expandedKey[i] << 4 : expandedKey[i];
        }
        return packed;
    }

    public byte[] packWithPrefix(int prefixVal) {
        if (prefixVal < 0 || prefixVal > 15) {
            throw new IllegalArgumentException("Invalid prefix value. Must be between 0 and 15, provided: " + prefixVal);
        }
        int length = expandedKey.length + 1;
        if (length % 2 != 0) {
            length++;
        }
        byte[] packed = new byte[length / 2];
        packed[0] |= prefixVal << 4;
        int shiftedCount = isOdd() ? 1 : 2;
        for (int i = 0; i < expandedKey.length; i++) {
            int p = i + shiftedCount;
            packed[p / 2] |= p % 2 == 0 ? expandedKey[i] << 4 : expandedKey[i];
        }
        return packed;
    }

    /**
     * Slice this Nibble
     *
     * @param from from position
     * @param to   to position
     * @return the new slices Nibble
     */
    public Nibble slice(int from, int to) {
        if (from == 0 && to == expandedKey.length) {
            return this;
        }
        byte[] newKey = Arrays.copyOfRange(expandedKey, from, to);
        return new Nibble(newKey);
    }


    /**
     * Extract a common prefix from 2 Nibbles.
     * The result is an array containing:
     * <ol>
     *     <li>The nibble containing the prefix of <i>a</i> and <i>b</i></li>
     *     <li>The Nibble <i>a</i> without the common prefix</li>
     *     <li>The Nibble <i>b</i> without the common prefix</li>
     * </ol>
     *
     * @param a the first Nibble
     * @param b the second Nibble
     * @return an array of size 3 (check above)
     */
    public static Nibble[] extractPrefix(Nibble a, Nibble b) {
        Nibble prefix = getCommonPrefix(a, b);
        if (prefix.size() == 0) {
            return new Nibble[]{EMPTY, a, b};
        }
        Nibble a2 = a.slice(prefix.size(), a.size());
        Nibble b2 = b.slice(prefix.size(), b.size());
        return new Nibble[]{prefix, a2, b2};
    }

    /**
     * Find the common prefix between 2 Nibbles
     *
     * @param a the first Nibble
     * @param b the second Nibble
     * @return the common prefix, or an empty Nibble if the they don't have any prefix in common
     */
    public static Nibble getCommonPrefix(Nibble a, Nibble b) {
        int commonPrefixPosition = Arrays.mismatch(a.expandedKey, b.expandedKey);
        if (commonPrefixPosition == 0) {
            return EMPTY;
        }
        if (commonPrefixPosition == -1) {
            return (a.size() < b.size() ? a : b);
        }
        return (a.size() < b.size() ? a : b).slice(0, commonPrefixPosition);
    }

    /**
     * Creates a nibble from a binary string
     *
     * @param key   the key
     * @param isOdd if true, the last 4 bits are excluded
     * @return the Nibble for this key
     */
    public static Nibble forKey(byte[] key, boolean isOdd) {
        if (key.length == 0) {
            return EMPTY;
        }
        byte[] expKey = isOdd ? new byte[key.length * 2 - 1] : new byte[key.length * 2];
        for (int i = 0; i < key.length; i++) {
            expKey[i * 2] = (byte) ((key[i] & 0xF0) >> 4);
            if (!isOdd || i < key.length - 1) {
                expKey[i * 2 + 1] = (byte) (key[i] & 0x0F);
            }
        }
        return new Nibble(expKey);
    }

    public static Nibble forKey(byte[] key) {
        return forKey(key, false);
    }

    public static Nibble forKey(String key) {
        return forKey(key.getBytes(), false);
    }

    @Override
    public int compareTo(Nibble o) {
        return Arrays.compareUnsigned(expandedKey, o.expandedKey);
    }

    private final static char[] TOSTRING_CHARS = "0123456789ABCDEF".toCharArray();

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < expandedKey.length; i++) {
            sb.append(TOSTRING_CHARS[expandedKey[i]]);
        }
        sb.append("]");
        return sb.toString();
    }
}
