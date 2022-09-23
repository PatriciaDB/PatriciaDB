package io.patriciadb.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public abstract class VarInt {

    public static final int MAX_VARINT_SIZE = 5;

    public static final int MAX_VARLONG_SIZE = 10;

    private VarInt() {
    }


    public static int varIntSize(int i) {
        int result = 0;
        do {
            result++;
            i >>>= 7;
        } while (i != 0);
        return result;
    }

    public static int getVarInt(byte[] src, int offset, int[] dst) {
        int result = 0;
        int shift = 0;
        int b;
        do {
            if (shift >= 32) {
                throw new IndexOutOfBoundsException("varint is too long");
            }
            b = src[offset++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        dst[0] = result;
        return offset;
    }

    public static int putVarInt(int v, byte[] sink, int offset) {
        do {
            int bits = v & 0x7F;
            v >>>= 7;
            byte b = (byte) (bits + ((v != 0) ? 0x80 : 0));
            sink[offset++] = b;
        } while (v != 0);
        return offset;
    }

    public static int getVarInt(ByteBuffer src) {
        int tmp;
        if ((tmp = src.get()) >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = src.get()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = src.get()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = src.get()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = src.get()) << 28;
                    while (tmp < 0) {
                        tmp = src.get();
                    }
                }
            }
        }
        return result;
    }

    public static int getVarInt(ByteBuffer src, int position) {
        int tmp;
        if ((tmp = src.get(position++)) >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = src.get(position++)) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = src.get(position++)) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = src.get(position++)) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = src.get(position++)) << 28;
                    while (tmp < 0) {
                        tmp = src.get(position++);
                    }
                }
            }
        }
        return result;
    }

    public static void putVarInt(int v, ByteBuffer sink) {
        while (true) {
            int bits = v & 0x7f;
            v >>>= 7;
            if (v == 0) {
                sink.put((byte) bits);
                return;
            }
            sink.put((byte) (bits | 0x80));
        }
    }

    public static int getVarInt(InputStream inputStream) throws IOException {
        int result = 0;
        int shift = 0;
        int b;
        do {
            if (shift >= 32) {
                throw new IndexOutOfBoundsException("varint is too long");
            }
            b = inputStream.read();
            if (b < 0) {
                throw new BufferUnderflowException();
            }
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    public static void putVarInt(int v, OutputStream outputStream) throws IOException {
        byte[] bytes = new byte[varIntSize(v)];
        putVarInt(v, bytes, 0);
        outputStream.write(bytes);
    }

    public static void putVarInt(int v, ByteArrayOutputStream bos) {
        while (true) {
            int bits = v & 0x7f;
            v >>>= 7;
            if (v == 0) {
                bos.write(bits);
                return;
            }
            bos.write(bits | 0x80);
        }
    }

    public static int varLongSize(long v) {
        int result = 0;
        do {
            result++;
            v >>>= 7;
        } while (v != 0);
        return result;
    }

    public static long getVarLong(ByteBuffer src) {
        long tmp;
        if ((tmp = src.get()) >= 0) {
            return tmp;
        }
        long result = tmp & 0x7f;
        if ((tmp = src.get()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = src.get()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = src.get()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    if ((tmp = src.get()) >= 0) {
                        result |= tmp << 28;
                    } else {
                        result |= (tmp & 0x7f) << 28;
                        if ((tmp = src.get()) >= 0) {
                            result |= tmp << 35;
                        } else {
                            result |= (tmp & 0x7f) << 35;
                            if ((tmp = src.get()) >= 0) {
                                result |= tmp << 42;
                            } else {
                                result |= (tmp & 0x7f) << 42;
                                if ((tmp = src.get()) >= 0) {
                                    result |= tmp << 49;
                                } else {
                                    result |= (tmp & 0x7f) << 49;
                                    if ((tmp = src.get()) >= 0) {
                                        result |= tmp << 56;
                                    } else {
                                        result |= (tmp & 0x7f) << 56;
                                        result |= ((long) src.get()) << 63;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public static long getVarLong(InputStream src) throws IOException {
        long result = 0;
        int shift = 0;
        long b;
        do {
            if (shift >= 64) {
                throw new IndexOutOfBoundsException("varint is too long");
            }
            b = src.read();
            if (b < 0) {
                throw new BufferUnderflowException();
            }
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    public static void putVarLong(long v, ByteBuffer sink) {
        while (true) {
            int bits = ((int) v) & 0x7f;
            v >>>= 7;
            if (v == 0) {
                sink.put((byte) bits);
                return;
            }
            sink.put((byte) (bits | 0x80));
        }
    }

    public static void putVarLong(long v, byte[] sink, int offset) {
        int i = offset;
        while (true) {
            int bits = ((int) v) & 0x7f;
            v >>>= 7;
            if (v == 0) {
                sink[i++] = (byte) bits;
                return;
            }
            sink[i++] = (byte) (bits | 0x80);
        }
    }

    public static void putVarLong(long v, OutputStream outputStream) throws IOException {
        while (true) {
            int bits = ((int) v) & 0x7f;
            v >>>= 7;
            if (v == 0) {
                outputStream.write(bits);
                return;
            }
            outputStream.write(bits | 0x80);
        }
    }

    public static void putVarLong(long v, ByteArrayOutputStream bos) {
        while (true) {
            int bits = ((int) v) & 0x7f;
            v >>>= 7;
            if (v == 0) {
                bos.write(bits);
                return;
            }
            bos.write(bits | 0x80);
        }
    }

    public static byte[] varLong(long v) {
        byte[] bytes = new byte[varLongSize(v)];
        putVarLong(v, bytes, 0);
        return bytes;
    }

    public static byte[] varInt(int v) {
        byte[] bytes = new byte[varIntSize(v)];
        putVarInt(v, bytes, 0);
        return bytes;
    }


    public static void putVarLong16(long v, ByteBuffer sink) {
        while (true) {
            int bits = ((int) v) & 0x7fff;
            v >>>= 15;
            if (v == 0) {
                sink.putShort((short) bits);
                return;
            }
            sink.putShort((short) (bits | 0x8000));
        }
    }

    public static void putVarLong16(long v, ByteArrayOutputStream bos) {
        while (true) {
            int bits = ((int) v) & 0x7fff;
            v >>>= 15;
            if (v == 0) {
                bos.write(bits>>>8);
                bos.write(bits&0xFF);
                return;
            }
            bos.write((bits>>>8)|0x80);
            bos.write(bits&0xFF);
        }
    }

    public static void putVarLong32(long v, ByteBuffer sink) {
        while (true) {
            int bits = ((int) v) & 0x7fffffff;
            v >>>= 31;
            if (v == 0) {
                sink.putInt(bits);
                return;
            }
            sink.putInt(bits | 0x80000000);
        }
    }

    public static long getVarLong32(ByteBuffer src) {
        long tmp;
        if ((tmp = src.getInt()) >= 0) {
            return tmp;
        }
        long result = tmp & 0x7fffffff;
        if ((tmp = src.getInt()) >= 0) {
            result |= tmp << 31;
        } else {
            result |= (tmp & 0x7fffffff) << 31;
            if ((tmp = src.getInt()) >= 0) {
                result |= tmp << 62;
            } else {
                result |= (tmp & 0x7fffffff) << 62;
            }
        }
        return result;
    }

    public static long getVarLong16(ByteBuffer src) {
        long tmp;
        if ((tmp = src.getShort()) >= 0) {
            return tmp;
        }
        long result = tmp & 0x7fff;
        if ((tmp = src.getShort()) >= 0) {
            result |= tmp << 15;
        } else {
            result |= (tmp & 0x7fff) << 15;
            if ((tmp = src.getShort()) >= 0) {
                result |= tmp << 30;
            } else {
                result |= (tmp & 0x7fff) << 30;
                if ((tmp = src.getShort()) >= 0) {
                    result |= tmp << 45;
                } else {
                    result |= (tmp & 0x7fff) << 45;
                    if ((tmp = src.getShort()) >= 0) {
                        result |= tmp << 60;
                    } else {
                        result |= (tmp & 0x7fff) << 60;
                    }
                }
            }
        }
        return result;
    }
}