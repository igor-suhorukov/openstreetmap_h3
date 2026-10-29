package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RowsInputStreamTest {

    private static final List<String> ROWS = List.of("1\tа\n", "", "22\tThai ไทย\n", "3\n");

    @Test
    void read_buffersAcrossRowsGiveUtf8OfAllRows() throws IOException {
        byte[] expected = String.join("", ROWS).getBytes(StandardCharsets.UTF_8);
        for (int size : new int[]{1, 3, 7, expected.length + 5}) {
            try (InputStream input = new RowsInputStream(ROWS.iterator())) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                byte[] buffer = new byte[size];
                int read;
                while ((read = input.read(buffer, 0, size)) >= 0) {
                    output.write(buffer, 0, read);
                }
                assertArrayEquals(expected, output.toByteArray(), "buffer " + size);
            }
        }
    }

    @Test
    void read_singleBytesAndOffsets() throws IOException {
        try (InputStream input = new RowsInputStream(List.of("ab", "ä").iterator())) {
            assertEquals('a', input.read());
            byte[] buffer = new byte[5];
            assertEquals(3, input.read(buffer, 1, 4));
            assertArrayEquals(new byte[]{0, 'b', (byte) 0xC3, (byte) 0xA4, 0}, buffer);
            assertEquals(-1, input.read());
            assertEquals(-1, input.read(buffer, 0, 5));
            assertEquals(0, input.read(buffer, 0, 0));
        }
    }

    @Test
    void read_emptyRowsAndInvalidRange() throws IOException {
        byte[] buffer = new byte[2];
        try (InputStream empty = new RowsInputStream(Collections.emptyIterator());
             InputStream emptyRows = new RowsInputStream(List.of("", "").iterator())) {
            assertEquals(-1, empty.read());
            assertEquals(-1, emptyRows.read(buffer, 0, 2));
            assertThrows(IndexOutOfBoundsException.class, () -> empty.read(buffer, 1, 2));
        }
    }
}
