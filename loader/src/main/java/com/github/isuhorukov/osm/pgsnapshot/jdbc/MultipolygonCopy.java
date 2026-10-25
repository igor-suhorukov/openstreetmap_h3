package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.PipedProcess;
import org.postgresql.copy.CopyIn;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

final class MultipolygonCopy {

    static final String COPY_SQL = "COPY preimport_multipolygon FROM STDIN";

    private static final byte[] RELATION_COLUMN = "relation\t".getBytes(StandardCharsets.US_ASCII);
    private static final int BUFFER_SIZE = 1 << 20;

    private final CopyIn copyIn;
    private byte[] buffer = new byte[BUFFER_SIZE];
    private int length;
    private int scanned;
    private int copyStart = -1;
    private long rows;

    private MultipolygonCopy(CopyIn copyIn) {
        this.copyIn = copyIn;
    }

    static long copyRelations(InputStream osmiumOutput, CopyIn copyIn) throws IOException, SQLException {
        return new MultipolygonCopy(copyIn).copy(osmiumOutput);
    }

    static long copyExport(Connection connection, PipedProcess export) throws IOException, SQLException, InterruptedException {
        CopyIn copyIn = PsqlRunner.copyManager(connection).copyIn(COPY_SQL);
        long rows = copyRelations(export.getInputStream(), copyIn);
        export.waitForSuccess();
        copyIn.endCopy();
        return rows;
    }

    private long copy(InputStream osmiumOutput) throws IOException, SQLException {
        int read;
        while ((read = osmiumOutput.read(buffer, length, buffer.length - length)) >= 0) {
            length += read;
            compact(copyCompleteLines());
        }
        if (length > 0 && isRelation(buffer, 0, length)) {
            copyIn.writeToCopy(buffer, 0, length);
            rows++;
        }
        return rows;
    }

    private int copyCompleteLines() throws SQLException {
        int lineStart = 0;
        int newline;
        while ((newline = indexOf(buffer, scanned, length, (byte) '\n')) >= 0) {
            if (isRelation(buffer, lineStart, newline)) {
                copyStart = copyStart < 0 ? lineStart : copyStart;
                rows++;
            } else {
                flush(lineStart);
            }
            lineStart = newline + 1;
            scanned = lineStart;
        }
        flush(lineStart);
        return lineStart;
    }

    private void flush(int end) throws SQLException {
        if (copyStart >= 0) {
            copyIn.writeToCopy(buffer, copyStart, end - copyStart);
            copyStart = -1;
        }
    }

    private void compact(int lineStart) {
        if (lineStart > 0) {
            length -= lineStart;
            System.arraycopy(buffer, lineStart, buffer, 0, length);
        }
        scanned = length;
        if (length == buffer.length) {
            buffer = Arrays.copyOf(buffer, buffer.length * 2);
        }
    }

    static boolean isRelation(byte[] buffer, int lineStart, int lineEnd) {
        int tab = indexOf(buffer, lineStart, lineEnd, (byte) '\t');
        if (tab < 0 || lineEnd - tab - 1 < RELATION_COLUMN.length) {
            return false;
        }
        for (int i = 0; i < RELATION_COLUMN.length; i++) {
            if (buffer[tab + 1 + i] != RELATION_COLUMN[i]) {
                return false;
            }
        }
        return true;
    }

    private static int indexOf(byte[] buffer, int from, int to, byte value) {
        for (int i = from; i < to; i++) {
            if (buffer[i] == value) {
                return i;
            }
        }
        return -1;
    }
}
