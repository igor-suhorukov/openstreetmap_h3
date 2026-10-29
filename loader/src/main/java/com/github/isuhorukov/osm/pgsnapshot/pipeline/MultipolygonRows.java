package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class MultipolygonRows<E extends Exception> {

    @FunctionalInterface
    public interface Sink<E extends Exception> {
        void write(byte[] buffer, int offset, int length) throws E;
    }

    private static final byte[] RELATION_COLUMN = "relation\t".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] NEWLINE = {'\n'};
    private static final int BUFFER_SIZE = 1 << 20;

    private final Sink<E> sink;
    private final MultipolygonTags multipolygonTags;
    private byte[] buffer = new byte[BUFFER_SIZE];
    private int length;
    private int scanned;
    private int copyStart = -1;
    private long rows;

    private MultipolygonRows(Sink<E> sink, MultipolygonTags multipolygonTags) {
        this.sink = sink;
        this.multipolygonTags = multipolygonTags;
    }

    public static <E extends Exception> long copyRelations(InputStream osmiumOutput, Sink<E> sink,
                                                           MultipolygonTags multipolygonTags) throws IOException, E {
        return new MultipolygonRows<>(sink, multipolygonTags).copy(osmiumOutput);
    }

    private long copy(InputStream osmiumOutput) throws IOException, E {
        int read;
        while ((read = osmiumOutput.read(buffer, length, buffer.length - length)) >= 0) {
            length += read;
            compact(copyCompleteLines());
        }
        if (length > 0 && isRelation(buffer, 0, length)) {
            rows++;
            byte[] enrichedTags = enrichedTags(0, length);
            if (enrichedTags.length == 0) {
                sink.write(buffer, 0, length);
            } else {
                writeEnriched(0, length, enrichedTags);
            }
        }
        return rows;
    }

    private int copyCompleteLines() throws E {
        int lineStart = 0;
        int newline;
        while ((newline = indexOf(buffer, scanned, length, (byte) '\n')) >= 0) {
            if (isRelation(buffer, lineStart, newline)) {
                rows++;
                byte[] enrichedTags = enrichedTags(lineStart, newline);
                if (enrichedTags.length == 0) {
                    copyStart = copyStart < 0 ? lineStart : copyStart;
                } else {
                    flush(lineStart);
                    writeEnriched(lineStart, newline, enrichedTags);
                    sink.write(NEWLINE, 0, NEWLINE.length);
                }
            } else {
                flush(lineStart);
            }
            lineStart = newline + 1;
            scanned = lineStart;
        }
        flush(lineStart);
        return lineStart;
    }

    private byte[] enrichedTags(int lineStart, int lineEnd) {
        return multipolygonTags.isEmpty() ? MultipolygonTags.UNCHANGED
                : multipolygonTags.enrichedTags(buffer, lineStart, lineEnd);
    }

    private void writeEnriched(int lineStart, int lineEnd, byte[] enrichedTags) throws E {
        int tagsStart = MultipolygonTags.tagsStart(buffer, lineStart, lineEnd);
        sink.write(buffer, lineStart, tagsStart - lineStart);
        sink.write(enrichedTags, 0, enrichedTags.length);
    }

    private void flush(int end) throws E {
        if (copyStart >= 0) {
            sink.write(buffer, copyStart, end - copyStart);
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
