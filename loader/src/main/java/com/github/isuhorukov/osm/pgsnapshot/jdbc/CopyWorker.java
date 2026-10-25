package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

abstract class CopyWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(CopyWorker.class);
    private static final CopyChunk END_OF_STREAM = new CopyChunk("", Collections.emptyList());
    private static final long SUBMIT_CHECK_SECONDS = 1;

    private final String name;
    private final Connection connection;
    private final CopyManager copyManager;
    private final BlockingQueue<CopyChunk> queue;
    private final long pendingBytesLimit;
    private final Consumer<Throwable> failureListener;
    private final Thread thread;
    private final Map<String, PendingContents> pendingContents = new LinkedHashMap<>();
    private volatile boolean aborted;
    private Exception failure;
    private boolean streamEnded;
    private long pendingBytes;
    private String currentTarget;
    private CopyIn currentCopy;
    private long copyCommands;
    private long copiedBytes;

    CopyWorker(String name, Connection connection, int queueCapacity, long pendingBytesLimit,
               Consumer<Throwable> failureListener) throws SQLException {
        this.name = name;
        this.connection = connection;
        this.copyManager = PsqlRunner.copyManager(connection);
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.pendingBytesLimit = pendingBytesLimit;
        this.failureListener = failureListener;
        this.thread = new Thread(this, name);
    }

    protected abstract void begin() throws SQLException, IOException;

    protected abstract String copySql(String target) throws SQLException;

    protected abstract void finish() throws SQLException, IOException;

    protected Connection getConnection() {
        return connection;
    }

    String getName() {
        return name;
    }

    void start() {
        thread.start();
    }

    void submit(CopyChunk chunk) throws InterruptedException {
        while (!queue.offer(chunk, SUBMIT_CHECK_SECONDS, TimeUnit.SECONDS)) {
            if (!thread.isAlive()) {
                throw new IllegalStateException("COPY worker " + name + " is stopped");
            }
        }
    }

    void endOfStream() throws InterruptedException {
        submit(END_OF_STREAM);
    }

    void abort() {
        aborted = true;
        thread.interrupt();
    }

    void join() throws InterruptedException {
        thread.join();
    }

    Exception getFailure() {
        return failure;
    }

    @Override
    public void run() {
        try {
            begin();
            stream();
            if (!aborted) {
                finish();
                log.info("{}: {} COPY commands, {} MB", name, copyCommands, copiedBytes / 1_000_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e);
        } catch (SQLException | IOException | RuntimeException e) {
            fail(e);
            if (!streamEnded) {
                discardUntilEndOfStream();
            }
        }
    }

    private void fail(Exception e) {
        failure = e;
        if (!aborted) {
            failureListener.accept(new IllegalStateException("COPY worker " + name + " failed", e));
        }
    }

    private void stream() throws SQLException, InterruptedException {
        try {
            List<CopyChunk> batch = new ArrayList<>();
            while (!streamEnded) {
                batch.add(queue.take());
                queue.drainTo(batch);
                streamEnded = batch.contains(END_OF_STREAM);
                for (CopyChunk chunk : batch) {
                    if (chunk != END_OF_STREAM) {
                        accept(chunk);
                    }
                }
                batch.clear();
            }
            while (!pendingContents.isEmpty()) {
                switchTo(pendingContents.keySet().iterator().next());
            }
            endCopy();
        } catch (SQLException | RuntimeException e) {
            cancelCopy(e);
            throw e;
        }
    }

    private void accept(CopyChunk chunk) throws SQLException {
        if (currentTarget == null) {
            startCopy(chunk.getTarget());
        }
        if (chunk.getTarget().equals(currentTarget)) {
            write(chunk.getContents());
            return;
        }
        PendingContents pending = pendingContents.computeIfAbsent(chunk.getTarget(), target -> new PendingContents());
        for (byte[] content : chunk.getContents()) {
            pending.contents.add(content);
            pending.bytes += content.length;
            pendingBytes += content.length;
        }
        if (pendingBytes > pendingBytesLimit) {
            switchTo(largestPendingTarget());
        }
    }

    private String largestPendingTarget() {
        String largestTarget = null;
        long largestBytes = -1;
        for (Map.Entry<String, PendingContents> pending : pendingContents.entrySet()) {
            if (pending.getValue().bytes > largestBytes) {
                largestTarget = pending.getKey();
                largestBytes = pending.getValue().bytes;
            }
        }
        return largestTarget;
    }

    private void switchTo(String target) throws SQLException {
        endCopy();
        startCopy(target);
        PendingContents pending = pendingContents.remove(target);
        pendingBytes -= pending.bytes;
        write(pending.contents);
    }

    private void startCopy(String target) throws SQLException {
        String sql = copySql(target);
        currentCopy = copyManager.copyIn(sql);
        currentTarget = target;
    }

    private void write(List<byte[]> contents) throws SQLException {
        for (byte[] content : contents) {
            currentCopy.writeToCopy(content, 0, content.length);
            copiedBytes += content.length;
        }
    }

    private void endCopy() throws SQLException {
        if (currentCopy != null) {
            CopyIn copyIn = currentCopy;
            currentCopy = null;
            currentTarget = null;
            copyIn.endCopy();
            copyCommands++;
        }
    }

    private void cancelCopy(Exception failure) {
        if (currentCopy != null && currentCopy.isActive()) {
            try {
                currentCopy.cancelCopy();
            } catch (SQLException cancelFailure) {
                failure.addSuppressed(cancelFailure);
            }
        }
        currentCopy = null;
        currentTarget = null;
    }

    private void discardUntilEndOfStream() {
        try {
            CopyChunk chunk;
            do {
                chunk = queue.take();
            } while (chunk != END_OF_STREAM);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class PendingContents {
        private final List<byte[]> contents = new ArrayList<>();
        private long bytes;
    }
}
