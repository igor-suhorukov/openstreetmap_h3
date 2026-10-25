package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

final class MultipolygonExport implements AutoCloseable {

    @FunctionalInterface
    interface Export {
        long run() throws IOException, InterruptedException;
    }

    private static final Runnable NO_CANCEL_ACTION = () -> { };

    private final Export export;
    private final Runnable cancelAction;
    private final FutureTask<Long> task;
    private final Thread thread;

    private MultipolygonExport(Export export, Runnable cancelAction, boolean parallel) {
        this.export = export;
        this.cancelAction = cancelAction;
        this.task = parallel ? new FutureTask<>(export::run) : null;
        this.thread = parallel ? new Thread(task, "multipolygon-export") : null;
    }

    static MultipolygonExport start(Export export, boolean parallel) {
        return start(export, NO_CANCEL_ACTION, parallel);
    }

    static MultipolygonExport start(Export export, Runnable cancelAction, boolean parallel) {
        MultipolygonExport multipolygonExport = new MultipolygonExport(export, cancelAction, parallel);
        if (multipolygonExport.thread != null) {
            multipolygonExport.thread.start();
        }
        return multipolygonExport;
    }

    long await() throws IOException, InterruptedException {
        if (task == null) {
            return export.run();
        }
        try {
            return task.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("Multipolygon export failed", cause);
        }
    }

    @Override
    public void close() {
        if (thread == null) {
            return;
        }
        if (task.cancel(true)) {
            cancelAction.run();
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
