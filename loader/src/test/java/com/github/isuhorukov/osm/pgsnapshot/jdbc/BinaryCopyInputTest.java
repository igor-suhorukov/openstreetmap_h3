package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class BinaryCopyInputTest {

    static byte[] binaryCopy(long[][] rows) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.write(new byte[]{'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0});
            output.writeInt(0);
            output.writeInt(3);
            output.write(new byte[]{9, 9, 9});
            for (long[] row : rows) {
                output.writeShort(row.length);
                output.writeInt(Long.BYTES);
                output.writeLong(row[0]);
                output.writeInt(Integer.BYTES);
                output.writeInt((int) row[1]);
                if (row.length == 3) {
                    output.writeInt(Long.BYTES);
                    output.writeLong(row[2]);
                }
            }
            output.writeShort(-1);
        }
        return bytes.toByteArray();
    }

    private static InputStream oneByteAtATime(byte[] bytes) {
        return new FilterInputStream(new ByteArrayInputStream(bytes)) {
            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return super.read(buffer, offset, Math.min(1, length));
            }
        };
    }

    @Test
    void nextTuple_rowsSplitAcrossReads_fieldsDecoded() throws IOException {
        BinaryCopyInput input = new BinaryCopyInput(oneByteAtATime(binaryCopy(new long[][]{
                {1L, 3, Long.MIN_VALUE}, {Long.MAX_VALUE, -2, -1L}})));

        assertEquals(3, input.nextTuple());
        assertEquals(1L, input.readLongField());
        assertEquals(3, input.readIntField());
        assertEquals(Long.MIN_VALUE, input.readLongField());
        assertEquals(3, input.nextTuple());
        assertEquals(Long.MAX_VALUE, input.readLongField());
        assertEquals(-2, input.readIntField());
        assertEquals(-1L, input.readLongField());
        assertEquals(BinaryCopyInput.END_OF_DATA, input.nextTuple());
    }

    @Test
    void constructor_textInput_throws() {
        ByteArrayInputStream text = new ByteArrayInputStream("1\t2\n3\t4\n\\.\n".getBytes());

        assertThrows(IOException.class, () -> new BinaryCopyInput(text));
    }

    @Test
    void readIntField_unexpectedFieldLength_throws() throws IOException {
        BinaryCopyInput input = new BinaryCopyInput(new ByteArrayInputStream(binaryCopy(new long[][]{{1L, 2}})));
        input.nextTuple();

        IOException exception = assertThrows(IOException.class, input::readIntField);
        assertTrue(exception.getMessage().contains("of 4 bytes, got 8"), exception.getMessage());
    }

    @Test
    void nextTuple_truncatedStream_throwsEof() throws IOException {
        byte[] bytes = binaryCopy(new long[][]{{1L, 2}});
        BinaryCopyInput input = new BinaryCopyInput(new ByteArrayInputStream(java.util.Arrays.copyOf(bytes, bytes.length - 4)));
        input.nextTuple();
        input.readLongField();

        assertThrows(EOFException.class, input::readIntField);
    }
}
