package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.admin.AdminDatabaseFiles;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminExport;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminUnits;
import com.github.isuhorukov.osm.pgsnapshot.arrow.AdminDirectoryArrow;
import com.github.isuhorukov.osm.pgsnapshot.arrow.PartitionWriter;
import com.github.isuhorukov.osm.pgsnapshot.output.ImportState;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

final class AdminUnitsOutput {

    @FunctionalInterface
    interface Task {
        void run() throws IOException;
    }

    private final AdminUnits adminUnits;
    private final String importState;
    private final Map<String, String> metadata = new TreeMap<>();
    private final List<Task> tasks = new ArrayList<>();

    AdminUnitsOutput(AdminUnits adminUnits, String importState) {
        this.adminUnits = adminUnits;
        this.importState = importState;
        adminUnits.finish();
        metadata.put(ImportState.METADATA_KEY, importState);
        metadata.put(AdminUnits.METADATA_KEY, adminUnits.reportMetadataJson());
    }

    AdminUnitsOutput database(File directory) throws IOException {
        Files.createDirectories(directory.toPath());
        tasks.add(() -> AdminDatabaseFiles.writeUnits(adminUnits, directory));
        tasks.add(() -> AdminDatabaseFiles.writeRegions(adminUnits, directory));
        tasks.add(() -> AdminDatabaseFiles.writeState(adminUnits, directory));
        return this;
    }

    AdminUnitsOutput arrow(PartitionWriter writer, File directory) throws IOException {
        Files.createDirectories(directory.toPath());
        tasks.add(() -> AdminDirectoryArrow.writeUnits(adminUnits.getDirectory(), writer, directory, metadata));
        tasks.add(() -> AdminDirectoryArrow.writeRegions(adminUnits.getDirectory(), writer, directory, metadata));
        return this;
    }

    AdminUnitsOutput export(File directory, PartitionWriter parquetWriter) throws IOException {
        Files.createDirectories(directory.toPath());
        tasks.add(() -> AdminExport.writeUnits(adminUnits, directory));
        tasks.add(() -> AdminExport.writeRegions(adminUnits, directory));
        tasks.add(() -> AdminExport.writeState(adminUnits, directory, importState, ImportState.METADATA_KEY));
        return arrow(parquetWriter, directory);
    }

    void write() throws IOException, InterruptedException {
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            FutureTask<Void> future = new FutureTask<>(() -> {
                task.run();
                return null;
            });
            new Thread(future, "admin-units-output-" + i).start();
            futures.add(future);
        }
        for (Future<Void> future : futures) {
            await(future);
        }
    }

    private static void await(Future<Void> future) throws IOException, InterruptedException {
        try {
            future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException("Writing the admin unit directory failed", cause);
        }
    }
}
