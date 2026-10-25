package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

final class BinaryCopyInput {

    static final int END_OF_DATA = -1;

    private static final byte[] SIGNATURE = {'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0};
    private static final int BUFFER_SIZE = 1 << 16;

    private final InputStream input;
    private final byte[] buffer = new byte[BUFFER_SIZE];
    private int position;
    private int limit;

    BinaryCopyInput(InputStream input) throws IOException {
        this.input = input;
        for (byte expected : SIGNATURE) {
            ensure(1);
            if (buffer[position++] != expected) {
                throw new IOException("Not a binary COPY stream");
            }
        }
        readInt();
        int extensionLength = readInt();
        for (int i = 0; i < extensionLength; i++) {
            ensure(1);
            position++;
        }
    }

    int nextTuple() throws IOException {
        ensure(Short.BYTES);
        short fieldCount = (short) (((buffer[position] & 0xFF) << 8) | (buffer[position + 1] & 0xFF));
        position += Short.BYTES;
        return fieldCount;
    }

    int readIntField() throws IOException {
        checkFieldLength(Integer.BYTES);
        return readInt();
    }

    long readLongField() throws IOException {
        checkFieldLength(Long.BYTES);
        ensure(Long.BYTES);
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << 8) | (buffer[position + i] & 0xFF);
        }
        position += Long.BYTES;
        return value;
    }

    private void checkFieldLength(int expected) throws IOException {
        int length = readInt();
        if (length != expected) {
            throw new IOException("Expected binary COPY field of " + expected + " bytes, got " + length);
        }
    }

    private int readInt() throws IOException {
        ensure(Integer.BYTES);
        int value = ((buffer[position] & 0xFF) << 24) | ((buffer[position + 1] & 0xFF) << 16)
                | ((buffer[position + 2] & 0xFF) << 8) | (buffer[position + 3] & 0xFF);
        position += Integer.BYTES;
        return value;
    }

    private void ensure(int bytes) throws IOException {
        if (limit - position >= bytes) {
            return;
        }
        int remaining = limit - position;
        System.arraycopy(buffer, position, buffer, 0, remaining);
        position = 0;
        limit = remaining;
        while (limit < bytes) {
            int read = input.read(buffer, limit, buffer.length - limit);
            if (read < 0) {
                throw new EOFException("Binary COPY stream ended unexpectedly");
            }
            limit += read;
        }
    }
}
