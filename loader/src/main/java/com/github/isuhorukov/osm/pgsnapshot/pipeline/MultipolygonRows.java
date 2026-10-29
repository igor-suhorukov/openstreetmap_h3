package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.admin.AdminBinder;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminBinding;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminColumns;
import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public final class MultipolygonRows {

    @FunctionalInterface
    public interface Sink<E extends Exception> {
        void write(byte[] buffer, int offset, int length) throws E;
    }

    @FunctionalInterface
    interface ResultSink<R, E extends Exception> {
        void accept(R result) throws E;
    }

    static final int MAX_BATCH_BYTES = 8 << 20;
    static final int MAX_BATCH_ROWS = 8192;
    private static final int BATCHES_IN_FLIGHT_PER_WORKER = 2;
    private static final byte[] RELATION_COLUMN = "relation\t".getBytes(StandardCharsets.US_ASCII);
    private static final int BUFFER_SIZE = 1 << 20;
    private static final int CELL_COLUMNS_BYTES = 16;

    private final InputStream osmiumOutput;
    private final int maxBatchBytes;
    private final int maxBatchRows;
    private byte[] buffer = new byte[BUFFER_SIZE];
    private int length;
    private int lineStart;
    private int scanned;
    private boolean endOfInput;
    private long rows;

    private MultipolygonRows(InputStream osmiumOutput, int maxBatchBytes, int maxBatchRows) {
        this.osmiumOutput = osmiumOutput;
        this.maxBatchBytes = maxBatchBytes;
        this.maxBatchRows = maxBatchRows;
    }

    public static <E extends Exception> long copyRelations(InputStream osmiumOutput, Sink<E> sink,
                                                           MultipolygonTags multipolygonTags, int workers)
            throws IOException, E {
        return copyRelations(osmiumOutput, sink, multipolygonTags, workers, null);
    }

    public static <E extends Exception> long copyRelations(InputStream osmiumOutput, Sink<E> sink,
                                                           MultipolygonTags multipolygonTags, int workers,
                                                           AdminBinder adminBinder) throws IOException, E {
        MultipolygonBatchProcessor processor = new MultipolygonBatchProcessor(multipolygonTags, true, null,
                GeometryH33.instance(), adminBinder);
        return processRelations(osmiumOutput, workers, processor::process, result -> {
            ByteBuffer rows = result.getTsvRows();
            sink.write(rows.array(), rows.arrayOffset() + rows.position(), rows.remaining());
        });
    }

    static <R, E extends Exception> long processRelations(InputStream osmiumOutput, int workers,
                                                          Function<Batch, R> processor, ResultSink<R, E> sink)
            throws IOException, E {
        return processRelations(osmiumOutput, workers, MAX_BATCH_BYTES, MAX_BATCH_ROWS, processor, sink);
    }

    static <R, E extends Exception> long processRelations(InputStream osmiumOutput, int workers, int maxBatchBytes,
                                                          int maxBatchRows, Function<Batch, R> processor,
                                                          ResultSink<R, E> sink) throws IOException, E {
        MultipolygonRows multipolygonRows = new MultipolygonRows(osmiumOutput, maxBatchBytes, maxBatchRows);
        if (workers <= 0) {
            Batch batch;
            while ((batch = multipolygonRows.nextBatch()) != null) {
                sink.accept(processor.apply(batch));
            }
            return multipolygonRows.rows;
        }
        try (Workers pool = new Workers(workers)) {
            Deque<Future<R>> pending = new ArrayDeque<>();
            Batch batch;
            while ((batch = multipolygonRows.nextBatch()) != null) {
                Batch submitted = batch;
                pending.add(pool.submit(() -> processor.apply(submitted)));
                if (pending.size() > workers * BATCHES_IN_FLIGHT_PER_WORKER) {
                    sink.accept(result(pending.remove()));
                }
            }
            while (!pending.isEmpty()) {
                sink.accept(result(pending.remove()));
            }
            return multipolygonRows.rows;
        }
    }

    private static <R> R result(Future<R> future) throws InterruptedIOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("multipolygon processing interrupted");
            interrupted.initCause(e);
            throw interrupted;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException("Multipolygon processing failed", cause);
        }
    }

    private Batch nextBatch() throws IOException {
        Batch batch = new Batch();
        while (batch.size() < maxBatchRows && batch.length() < maxBatchBytes) {
            int newline = indexOf(buffer, scanned, length, (byte) '\n');
            if (newline >= 0) {
                addIfRelation(batch, newline, newline + 1);
            } else if (endOfInput) {
                addIfRelation(batch, length, length);
                break;
            } else {
                scanned = length;
                read();
            }
        }
        return batch.size() == 0 ? null : batch;
    }

    private void addIfRelation(Batch batch, int lineEnd, int nextLine) {
        if (isRelation(buffer, lineStart, lineEnd)) {
            rows++;
            batch.add(buffer, lineStart, lineEnd, nextLine);
        }
        lineStart = nextLine;
        scanned = nextLine;
    }

    private void read() throws IOException {
        if (lineStart > 0) {
            length -= lineStart;
            System.arraycopy(buffer, lineStart, buffer, 0, length);
            scanned -= lineStart;
            lineStart = 0;
        }
        if (length == buffer.length) {
            buffer = Arrays.copyOf(buffer, buffer.length * 2);
        }
        int read = osmiumOutput.read(buffer, length, buffer.length - length);
        if (read < 0) {
            endOfInput = true;
        } else {
            length += read;
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

    static byte[] hexBytes(byte[] row, int start, int end, long id) {
        if ((end - start) % 2 != 0) {
            throw new IllegalStateException("Odd length of hex WKB of multipolygon " + id);
        }
        byte[] bytes = new byte[(end - start) / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (hexDigit(row[start + 2 * i], id) << 4 | hexDigit(row[start + 2 * i + 1], id));
        }
        return bytes;
    }

    private static int hexDigit(byte digit, long id) {
        if (digit >= '0' && digit <= '9') {
            return digit - '0';
        }
        if (digit >= 'A' && digit <= 'F') {
            return digit - 'A' + 10;
        }
        if (digit >= 'a' && digit <= 'f') {
            return digit - 'a' + 10;
        }
        throw new IllegalStateException("Invalid hex WKB of multipolygon " + id);
    }

    static int indexOf(byte[] buffer, int from, int to, byte value) {
        for (int i = from; i < to; i++) {
            if (buffer[i] == value) {
                return i;
            }
        }
        return -1;
    }

    static final class Batch {
        private static final int INITIAL_CAPACITY = 1 << 16;
        private static final int INITIAL_ROWS = 64;

        private byte[] bytes = new byte[INITIAL_CAPACITY];
        private int length;
        private int[] starts = new int[INITIAL_ROWS];
        private int[] ends = new int[INITIAL_ROWS];
        private int size;

        private void add(byte[] source, int start, int end, int next) {
            int rowLength = next - start;
            if (length + rowLength > bytes.length) {
                bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, length + rowLength));
            }
            System.arraycopy(source, start, bytes, length, rowLength);
            if (size == starts.length) {
                starts = Arrays.copyOf(starts, size * 2);
                ends = Arrays.copyOf(ends, size * 2);
            }
            starts[size] = length;
            ends[size] = length + end - start;
            size++;
            length += rowLength;
        }

        int size() {
            return size;
        }

        int length() {
            return length;
        }

        byte[] bytes() {
            return bytes;
        }

        int start(int row) {
            return starts[row];
        }

        int end(int row) {
            return ends[row];
        }

        Geometry[] geometries() {
            WKBReader wkbReader = new WKBReader();
            Geometry[] geometries = new Geometry[size];
            for (int row = 0; row < size; row++) {
                geometries[row] = geometry(row, wkbReader);
            }
            return geometries;
        }

        private Geometry geometry(int row, WKBReader wkbReader) {
            long id = MultipolygonTags.relationId(bytes, starts[row], ends[row]);
            int wkbEnd = indexOf(bytes, starts[row], ends[row], (byte) '\t');
            try {
                return wkbReader.read(hexBytes(bytes, starts[row], wkbEnd, id));
            } catch (ParseException e) {
                throw new IllegalStateException("Invalid WKB of multipolygon " + id, e);
            }
        }

        ByteBuffer importRows(MultipolygonTags multipolygonTags, short[][] cells, AdminBinding[] bindings,
                              AdminColumns adminColumns) {
            ByteArrayOutputStream rows = new ByteArrayOutputStream(length + length / 8 + size * CELL_COLUMNS_BYTES);
            for (int row = 0; row < size; row++) {
                byte[] tags = multipolygonTags.isEmpty() ? MultipolygonTags.UNCHANGED
                        : multipolygonTags.enrichedTags(bytes, starts[row], ends[row]);
                if (tags.length > 0) {
                    int tagsStart = MultipolygonTags.tagsStart(bytes, starts[row], ends[row]);
                    rows.write(bytes, starts[row], tagsStart - starts[row]);
                    rows.write(tags, 0, tags.length);
                } else {
                    rows.write(bytes, starts[row], ends[row] - starts[row]);
                }
                byte[] cellColumns = cellColumns(cells[row], bindings != null ? bindings[row] : null, adminColumns);
                rows.write(cellColumns, 0, cellColumns.length);
            }
            return ByteBuffer.wrap(rows.toByteArray());
        }

        private static byte[] cellColumns(short[] cells, AdminBinding binding, AdminColumns adminColumns) {
            StringBuilder columns = new StringBuilder(CELL_COLUMNS_BYTES).append('\t').append(GeometryH33.h33(cells))
                    .append('\t');
            if (cells.length > 1) {
                columns.append('{');
                for (int i = 0; i < cells.length; i++) {
                    columns.append(i == 0 ? "" : ",").append(cells[i]);
                }
                columns.append('}');
            } else {
                columns.append("\\N");
            }
            AdminBinding.appendTsv(columns, binding, adminColumns);
            return columns.append('\n').toString().getBytes(StandardCharsets.US_ASCII);
        }
    }

    private static final class Workers implements AutoCloseable {
        private final AtomicInteger threads = new AtomicInteger();
        private final ExecutorService executor;

        private Workers(int count) {
            executor = Executors.newFixedThreadPool(count, this::newThread);
        }

        private Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "multipolygon-" + threads.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }

        private <R> Future<R> submit(Callable<R> task) {
            return executor.submit(task);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
