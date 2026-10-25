package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

final class MultipolygonExport implements AutoCloseable {

    @FunctionalInterface
    interface Export {
        long run() throws IOException, InterruptedException;
    }

    private final Export export;
    private final FutureTask<Long> task;
    private final Thread thread;

    private MultipolygonExport(Export export, boolean parallel) {
        this.export = export;
        this.task = parallel ? new FutureTask<>(export::run) : null;
        this.thread = parallel ? new Thread(task, "multipolygon-export") : null;
    }

    static MultipolygonExport start(Export export, boolean parallel) {
        MultipolygonExport multipolygonExport = new MultipolygonExport(export, parallel);
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
        task.cancel(true);
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
