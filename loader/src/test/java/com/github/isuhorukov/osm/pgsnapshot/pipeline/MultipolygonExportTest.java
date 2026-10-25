package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MultipolygonExportTest {

    @Test
    void await_parallel_returnsExportTimeFromBackgroundThread() throws Exception {
        AtomicReference<Thread> exportThread = new AtomicReference<>();

        try (MultipolygonExport export = MultipolygonExport.start(() -> {
            exportThread.set(Thread.currentThread());
            return 42;
        }, true)) {
            assertEquals(42, export.await());
        }
        assertNotSame(Thread.currentThread(), exportThread.get());
        assertEquals("multipolygon-export", exportThread.get().getName());
    }

    @Test
    void await_notParallel_runsExportInCallerOnlyWhenAwaited() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<Thread> exportThread = new AtomicReference<>();

        try (MultipolygonExport export = MultipolygonExport.start(() -> {
            calls.incrementAndGet();
            exportThread.set(Thread.currentThread());
            return 7;
        }, false)) {
            assertEquals(0, calls.get());
            assertEquals(7, export.await());
        }
        assertEquals(1, calls.get());
        assertSame(Thread.currentThread(), exportThread.get());
    }

    @Test
    void close_notParallelWithoutAwait_exportNeverRuns() {
        AtomicInteger calls = new AtomicInteger();

        MultipolygonExport.start(calls::incrementAndGet, false).close();

        assertEquals(0, calls.get());
    }

    @Test
    void await_exportThrowsIOException_rethrowsIt() {
        IOException failure = new IOException("osmium failed");

        try (MultipolygonExport export = MultipolygonExport.start(() -> {
            throw failure;
        }, true)) {
            assertSame(failure, assertThrows(IOException.class, export::await));
        }
    }

    @Test
    void await_exportThrowsRuntimeException_rethrowsIt() {
        IllegalStateException failure = new IllegalStateException("exit code 1");

        try (MultipolygonExport export = MultipolygonExport.start(() -> {
            throw failure;
        }, true)) {
            assertSame(failure, assertThrows(IllegalStateException.class, export::await));
        }
    }

    @Test
    void await_exportThrowsError_wrapsIt() {
        LinkageError failure = new LinkageError("duckdb");

        try (MultipolygonExport export = MultipolygonExport.start(() -> {
            throw failure;
        }, true)) {
            IllegalStateException exception = assertThrows(IllegalStateException.class, export::await);
            assertEquals("Multipolygon export failed", exception.getMessage());
            assertSame(failure, exception.getCause());
        }
    }

    @Test
    void close_runningExport_interruptsAndWaitsForIt() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicBoolean finished = new AtomicBoolean();

        MultipolygonExport export = MultipolygonExport.start(() -> {
            started.countDown();
            try {
                return neverReleased.await(5, TimeUnit.MINUTES) ? 1 : 0;
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            } finally {
                finished.set(true);
            }
        }, true);
        assertTrue(started.await(10, TimeUnit.SECONDS));

        assertTimeoutPreemptively(Duration.ofSeconds(10), export::close);

        assertTrue(interrupted.get());
        assertTrue(finished.get());
    }
}
