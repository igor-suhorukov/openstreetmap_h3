package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;

final class RowsInputStream extends InputStream {

    private static final byte[] EMPTY = new byte[0];

    private final Iterator<String> rows;
    private byte[] row = EMPTY;
    private int position;

    RowsInputStream(Iterator<String> rows) {
        this.rows = rows;
    }

    @Override
    public int read() {
        return nextRow() ? row[position++] & 0xFF : -1;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return 0;
        }
        int copied = 0;
        while (copied < length && nextRow()) {
            int count = Math.min(length - copied, row.length - position);
            System.arraycopy(row, position, buffer, offset + copied, count);
            position += count;
            copied += count;
        }
        return copied == 0 ? -1 : copied;
    }

    private boolean nextRow() {
        while (position == row.length) {
            if (!rows.hasNext()) {
                return false;
            }
            row = rows.next().getBytes(StandardCharsets.UTF_8);
            position = 0;
        }
        return true;
    }
}
