package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CopyWorkerTest {

    private final List<Throwable> failures = new CopyOnWriteArrayList<>();
    private final Map<String, ByteArrayOutputStream> copies = new ConcurrentHashMap<>();
    private final List<String> copyCommands = new CopyOnWriteArrayList<>();
    private Connection connection;
    private CopyManager copyManager;

    private static final class RecordingWorker extends CopyWorker {
        private final List<String> events = new CopyOnWriteArrayList<>();
        private final String failingStep;

        private RecordingWorker(Connection connection, int queueCapacity, long pendingBytesLimit,
                                Consumer<Throwable> failureListener, String failingStep) throws SQLException {
            super("test-worker", connection, queueCapacity, pendingBytesLimit, failureListener);
            this.failingStep = failingStep;
        }

        private void event(String event) throws SQLException {
            events.add(event);
            if (event.equals(failingStep)) {
                throw new SQLException(event + " failed");
            }
        }

        @Override
        protected void begin() throws SQLException {
            event("begin");
        }

        @Override
        protected String copySql(String target) throws SQLException {
            event("copySql " + target);
            return "COPY " + target + " FROM STDIN";
        }

        @Override
        protected void finish() throws SQLException {
            event("finish");
        }
    }

    @BeforeEach
    void mockConnection() throws Exception {
        connection = mock(Connection.class);
        PGConnection pgConnection = mock(PGConnection.class);
        copyManager = mock(CopyManager.class);
        when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
        when(pgConnection.getCopyAPI()).thenReturn(copyManager);
        when(copyManager.copyIn(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            copyCommands.add(sql);
            ByteArrayOutputStream copied = copies.computeIfAbsent(sql, key -> new ByteArrayOutputStream());
            CopyIn copyIn = mock(CopyIn.class);
            doAnswer(write -> {
                copied.write(write.getArgument(0), write.getArgument(1), write.getArgument(2));
                return null;
            }).when(copyIn).writeToCopy(any(byte[].class), anyInt(), anyInt());
            return copyIn;
        });
    }

    private RecordingWorker worker(int queueCapacity, String failingStep) throws SQLException {
        return worker(queueCapacity, 1 << 20, failingStep);
    }

    private RecordingWorker worker(int queueCapacity, long pendingBytesLimit, String failingStep) throws SQLException {
        return new RecordingWorker(connection, queueCapacity, pendingBytesLimit, failures::add, failingStep);
    }

    private static CopyChunk chunk(String target, String... rows) {
        List<byte[]> contents = new java.util.ArrayList<>();
        for (String row : rows) {
            contents.add(row.getBytes(StandardCharsets.UTF_8));
        }
        return new CopyChunk(target, contents);
    }

    private String copied(String target) {
        return copies.get("COPY " + target + " FROM STDIN").toString(StandardCharsets.UTF_8);
    }

    @Test
    void run_queuedChunks_oneCopyPerTargetThenFinish() throws Exception {
        RecordingWorker worker = worker(8, null);
        worker.submit(chunk("a", "a1\n", "a2\n"));
        worker.submit(chunk("b", "b1\n"));
        worker.submit(chunk("a", "a3\n"));
        worker.endOfStream();

        worker.start();
        assertTimeoutPreemptively(Duration.ofSeconds(10), worker::join);

        assertEquals(List.of("begin", "copySql a", "copySql b", "finish"), worker.events);
        assertEquals(List.of("COPY a FROM STDIN", "COPY b FROM STDIN"), copyCommands);
        assertEquals("a1\na2\na3\n", copied("a"));
        assertEquals("b1\n", copied("b"));
        assertNull(worker.getFailure());
        assertTrue(failures.isEmpty());
        assertEquals("test-worker", worker.getName());
    }

    @Test
    void run_pendingBytesOverLimit_switchesToLargestPendingTarget() throws Exception {
        RecordingWorker worker = worker(8, 6, null);
        worker.submit(chunk("a", "a1\n"));
        worker.submit(chunk("b", "b1\n"));
        worker.submit(chunk("c", "c1\n", "c2\n"));
        worker.submit(chunk("a", "a2\n"));
        worker.submit(chunk("c", "c3\n"));
        worker.endOfStream();

        worker.start();
        assertTimeoutPreemptively(Duration.ofSeconds(10), worker::join);

        assertEquals(List.of("COPY a FROM STDIN", "COPY c FROM STDIN", "COPY b FROM STDIN", "COPY a FROM STDIN"),
                copyCommands);
        assertEquals("a1\na2\n", copied("a"));
        assertEquals("b1\n", copied("b"));
        assertEquals("c1\nc2\nc3\n", copied("c"));
        assertTrue(failures.isEmpty());
    }

    @Test
    void pendingBytesLimit_boundedByHeapShare() {
        assertEquals(128L << 20, PostgresqlLoader.pendingBytesLimit(10, 64L << 30));
        assertEquals(50L << 20, PostgresqlLoader.pendingBytesLimit(10, 2000L << 20));
        assertEquals(1L << 20, PostgresqlLoader.pendingBytesLimit(100, 64L << 20));
        assertEquals(128L << 20, PostgresqlLoader.pendingBytesLimit(0, 64L << 30));
    }

    @Test
    void run_copyFails_failureReportedWithoutFinish() throws Exception {
        RecordingWorker worker = worker(8, "copySql bad");
        worker.submit(chunk("bad", "x\n"));
        worker.submit(chunk("good", "y\n"));
        worker.endOfStream();

        worker.start();
        assertTimeoutPreemptively(Duration.ofSeconds(10), worker::join);

        assertFalse(worker.events.contains("finish"));
        assertInstanceOf(SQLException.class, worker.getFailure());
        assertEquals(1, failures.size());
        assertSame(worker.getFailure(), failures.get(0).getCause());
    }

    @Test
    void run_beginFails_chunksDiscardedUntilEndOfStream() throws Exception {
        RecordingWorker worker = worker(1, "begin");
        worker.start();

        worker.submit(chunk("a", "a1\n"));
        worker.submit(chunk("a", "a2\n"));
        worker.endOfStream();
        assertTimeoutPreemptively(Duration.ofSeconds(10), worker::join);

        assertEquals(List.of("begin"), worker.events);
        assertTrue(copyCommands.isEmpty());
        assertEquals(1, failures.size());
    }

    @Test
    void run_finishFails_failureReportedWithoutWaiting() throws Exception {
        RecordingWorker worker = worker(4, "finish");
        worker.submit(chunk("a", "a1\n"));
        worker.endOfStream();

        worker.start();
        assertTimeoutPreemptively(Duration.ofSeconds(10), worker::join);

        assertEquals("finish failed", worker.getFailure().getMessage());
        assertEquals(1, failures.size());
    }

    @Test
    void abort_waitingWorker_stopsWithoutFinishAndFailureReport() throws Exception {
        RecordingWorker worker = worker(4, null);
        worker.start();
        worker.submit(chunk("a", "a1\n"));

        worker.abort();
        assertTimeoutPreemptively(Duration.ofSeconds(10), worker::join);

        assertFalse(worker.events.contains("finish"));
        assertTrue(failures.isEmpty());
    }

    @Test
    void submit_stoppedWorkerWithFullQueue_throws() throws Exception {
        RecordingWorker worker = worker(1, null);
        worker.endOfStream();
        worker.start();
        assertTimeoutPreemptively(Duration.ofSeconds(10), worker::join);
        worker.submit(chunk("a", "a1\n"));
        CopyChunk secondChunk = chunk("a", "a2\n");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> worker.submit(secondChunk));

        assertEquals("COPY worker test-worker is stopped", exception.getMessage());
    }

    @Test
    void run_copyStreamFails_copyCancelled() throws Exception {
        CopyIn failingCopy = mock(CopyIn.class);
        doThrow(new SQLException("broken pipe")).when(failingCopy).writeToCopy(any(byte[].class), anyInt(), anyInt());
        when(failingCopy.isActive()).thenReturn(true);
        doThrow(new SQLException("cancel failed")).when(failingCopy).cancelCopy();
        doReturn(failingCopy).when(copyManager).copyIn(anyString());
        RecordingWorker worker = worker(4, null);
        worker.submit(chunk("a", "a1\n"));
        worker.endOfStream();

        worker.start();
        assertTimeoutPreemptively(Duration.ofSeconds(10), worker::join);

        verify(failingCopy).cancelCopy();
        assertEquals("broken pipe", worker.getFailure().getMessage());
        assertEquals("cancel failed", worker.getFailure().getSuppressed()[0].getMessage());
    }
}
