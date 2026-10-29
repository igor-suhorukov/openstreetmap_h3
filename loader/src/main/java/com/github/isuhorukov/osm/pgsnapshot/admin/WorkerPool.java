package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

final class WorkerPool implements AutoCloseable {

    private final ForkJoinPool pool;

    WorkerPool(int threads) {
        this.pool = new ForkJoinPool(Math.max(1, threads));
    }

    void parallel(int count, IntConsumer action) throws InterruptedException {
        try {
            pool.submit(() -> IntStream.range(0, count).parallel().forEach(action)).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException("Admin unit directory build failed", cause);
        }
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }
}
