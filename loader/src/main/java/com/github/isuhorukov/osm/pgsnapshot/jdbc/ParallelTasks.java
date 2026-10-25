package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

final class ParallelTasks {

    @FunctionalInterface
    interface Task {
        void run() throws IOException, SQLException;
    }

    private ParallelTasks() {
    }

    static void run(Map<String, Task> tasks) throws IOException, InterruptedException, SQLException {
        List<Future<Void>> futures = new ArrayList<>();
        for (Map.Entry<String, Task> task : tasks.entrySet()) {
            FutureTask<Void> future = new FutureTask<>(() -> {
                task.getValue().run();
                return null;
            });
            new Thread(future, task.getKey()).start();
            futures.add(future);
        }
        for (Future<Void> future : futures) {
            await(future);
        }
    }

    static <T> T await(Future<T> future) throws IOException, InterruptedException, SQLException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException) {
                throw (SQLException) cause;
            }
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException(cause);
        }
    }
}
