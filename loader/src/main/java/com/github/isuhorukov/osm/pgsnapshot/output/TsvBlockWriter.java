package com.github.isuhorukov.osm.pgsnapshot.output;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class TsvBlockWriter extends OrderedBlockWriter {

    private final ExecutorService saveExecutorService;
    private final File resultDirectory;

    public TsvBlockWriter(ExecutorService saveExecutorService, File resultDirectory, int maxBlocksInFlight,
                          Consumer<Throwable> failureListener) {
        super(maxBlocksInFlight, failureListener);
        this.saveExecutorService = saveExecutorService;
        this.resultDirectory = resultDirectory;
    }

    @Override
    protected void write(EncodedBlock block) throws InterruptedException, ExecutionException {
        File directory = new File(resultDirectory, block.getType().getDirectoryName());
        List<Future<?>> saveFutures = new ArrayList<>();
        for (Map.Entry<Short, byte[]> content : block.getContents().entrySet()) {
            saveFutures.add(saveExecutorService.submit(() -> append(directory, content.getKey(), content.getValue())));
        }
        for (Future<?> future : saveFutures) {
            future.get();
        }
    }

    private static void append(File directory, short h33, byte[] content) {
        try (OutputStream dataExport = new FileOutputStream(new File(directory, String.format("%05d.tsv", h33)), true)) {
            dataExport.write(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
