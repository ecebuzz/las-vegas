package edu.brown.lasvegas.util;

import java.io.ByteArrayOutputStream;

/**
 * Extends {@link ByteArrayOutputStream} just for using the given byte array
 * and returning it without copying.
 */
public final class RawByteArrayOutputStream extends ByteArrayOutputStream {
    public RawByteArrayOutputStream(byte[] buffer) {
        this (buffer, 0);
    }
    public RawByteArrayOutputStream(byte[] buffer, int offset) {
        buf = buffer;
        count = offset;
    }
    public byte[] getRawBuffer () {
        return buf;
    }
}