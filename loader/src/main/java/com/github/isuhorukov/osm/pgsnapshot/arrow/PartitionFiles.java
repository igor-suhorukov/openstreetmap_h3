package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.model.Partition;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import com.github.isuhorukov.osm.pgsnapshot.util.GeometryH33;
import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public final class PartitionFiles {

    private static final Logger log = LoggerFactory.getLogger(PartitionFiles.class);
    private static final String MULTIPOLYGON = "multipolygon";
    private static final List<String> MULTIPOLYGON_ORDER = List.of(ArrowSchemas.H33, ArrowSchemas.ID);

    private final File arrowDirectory;
    private final File spillDirectory;
    private final List<Partition> partitions;
    private final ArrowSpill arrowSpill;
    private final PartitionWriter blockWriter;
    private final PartitionWriter multipolygonWriter;
    private final Map<String, String> dataMetadata;
    private final Map<String, String> relationMetadata;

    public PartitionFiles(File arrowDirectory, List<Partition> partitions, ArrowSpill arrowSpill,
                          PartitionWriter blockWriter, PartitionWriter multipolygonWriter) {
        this(arrowDirectory, partitions, arrowSpill, blockWriter, multipolygonWriter, Map.of(), Map.of());
    }

    public PartitionFiles(File arrowDirectory, List<Partition> partitions, ArrowSpill arrowSpill,
                          PartitionWriter blockWriter, PartitionWriter multipolygonWriter,
                          Map<String, String> dataMetadata, Map<String, String> relationMetadata) {
        this.arrowDirectory = arrowDirectory;
        this.spillDirectory = new File(arrowDirectory, ResultLayout.SPILL_DIR);
        this.partitions = List.copyOf(partitions);
        this.arrowSpill = arrowSpill;
        this.blockWriter = blockWriter;
        this.multipolygonWriter = multipolygonWriter;
        this.dataMetadata = Map.copyOf(dataMetadata);
        this.relationMetadata = Map.copyOf(relationMetadata);
    }

    public File getSpillDirectory() {
        return spillDirectory;
    }

    public File getArrowDirectory() {
        return arrowDirectory;
    }

    public PartitionWriter getBlockWriter() {
        return blockWriter;
    }

    public ArrowSpill getArrowSpill() {
        return arrowSpill;
    }

    public void writeBlocks(ExecutorService executor) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        List<PartitionFile> files = new ArrayList<>();
        files.addAll(partitionFiles(ResultLayout.NODES_DIR, PartitionSplitter.NODES,
                ArrowSchemas.getNodesOrWaysSchema(false, arrowSpill.getAdminColumns()), false, blockWriter));
        files.addAll(partitionFiles(ResultLayout.WAYS_DIR, PartitionSplitter.WAYS,
                ArrowSchemas.getNodesOrWaysSchema(true, arrowSpill.getAdminColumns()), true, blockWriter));
        List<File> relationSpill = new ArrayList<>(spillFiles(ResultLayout.RELATIONS_DIR).values());
        if (!relationSpill.isEmpty()) {
            files.add(new PartitionFile(new File(new File(arrowDirectory, ResultLayout.RELATIONS_DIR),
                    ResultLayout.RELATIONS_DIR + blockWriter.fileExtension()), ArrowSchemas.getRelationSchema(),
                    relationSpill, blockWriter, relationMetadata));
        }
        files.sort(Comparator.comparingLong(PartitionFile::getSpilledBytes).reversed());
        List<Future<?>> writes = new ArrayList<>();
        for (PartitionFile file : files) {
            writes.add(executor.submit(() -> {
                write(file, List.of());
                return null;
            }));
        }
        awaitAll(writes);
        log.info("{} partition files of nodes, ways and relations written in {} ms", files.size(),
                System.currentTimeMillis() - start);
    }

    public void appendMultipolygons(Map<Short, byte[]> chunks) throws IOException {
        File directory = new File(spillDirectory, ResultLayout.MULTIPOLYGON_DIR);
        for (Map.Entry<Short, byte[]> chunk : chunks.entrySet()) {
            append(new File(directory, ResultLayout.spillFileName(chunk.getKey())), chunk.getValue());
        }
    }

    public void writeMultipolygons() throws IOException {
        long start = System.currentTimeMillis();
        List<PartitionFile> files = partitionFiles(ResultLayout.MULTIPOLYGON_DIR, MULTIPOLYGON,
                ArrowSchemas.getMultipolygonSchema(arrowSpill.getAdminColumns()), true, multipolygonWriter);
        for (PartitionFile file : files) {
            write(file, MULTIPOLYGON_ORDER);
        }
        log.info("{} partition files of multipolygons written in {} ms", files.size(),
                System.currentTimeMillis() - start);
    }

    public void deleteSpill() {
        for (String entityDir : ResultLayout.SPILL_ENTITY_DIRS) {
            File directory = new File(spillDirectory, entityDir);
            for (File spillFile : spillFiles(entityDir).values()) {
                delete(spillFile);
            }
            delete(directory);
        }
        delete(spillDirectory);
    }

    private List<PartitionFile> partitionFiles(String entityDir, String tableName, Schema schema,
                                               boolean multipleCellsPartition, PartitionWriter writer) {
        NavigableMap<Short, File> cells = spillFiles(entityDir);
        File targetDirectory = new File(arrowDirectory, entityDir);
        List<PartitionFile> files = new ArrayList<>();
        for (Partition partition : partitions) {
            NavigableMap<Short, File> partitionCells = cells.subMap(partition.getMinRange(), true,
                    partition.getMaxRange(), false);
            if (!partitionCells.isEmpty()) {
                files.add(new PartitionFile(new File(targetDirectory,
                        PartitionSplitter.partitionTableName(tableName, partition.getId()) + writer.fileExtension()),
                        schema, new ArrayList<>(partitionCells.values()), writer, dataMetadata));
                partitionCells.clear();
            }
        }
        File multipleCells = multipleCellsPartition ? cells.remove(GeometryH33.MULTIPLE_CELLS) : null;
        if (multipleCells != null) {
            files.add(new PartitionFile(new File(targetDirectory, tableName + "_" + GeometryH33.MULTIPLE_CELLS
                    + writer.fileExtension()), schema, List.of(multipleCells), writer, dataMetadata));
        }
        if (!cells.isEmpty()) {
            throw new IllegalStateException("No partition for " + entityDir + " with h3_3 " + cells.keySet());
        }
        return files;
    }

    private NavigableMap<Short, File> spillFiles(String entityDir) {
        NavigableMap<Short, File> cells = new TreeMap<>();
        File[] files = new File(spillDirectory, entityDir)
                .listFiles((directory, name) -> name.endsWith(ResultLayout.SPILL_FILE_EXTENSION));
        if (files != null) {
            for (File file : files) {
                String name = file.getName();
                cells.put(Short.parseShort(name.substring(0, name.length() - ResultLayout.SPILL_FILE_EXTENSION.length())),
                        file);
            }
        }
        return cells;
    }

    private void write(PartitionFile file, List<String> orderBy) throws IOException {
        long start = System.currentTimeMillis();
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = new CellBatchReader(allocator, file.schema, file.spillFiles,
                     CellBatchReader.MAX_BATCH_ROWS)) {
            file.writer.write(allocator, reader, file.target, orderBy, file.metadata);
        }
        for (File spillFile : file.spillFiles) {
            delete(spillFile);
        }
        log.info("{} written from {} h3_3 cells, {} spilled bytes, in {} ms", file.target.getName(),
                file.spillFiles.size(), file.spilledBytes, System.currentTimeMillis() - start);
    }

    private static void awaitAll(List<Future<?>> writes) throws IOException, InterruptedException {
        try {
            for (Future<?> write : writes) {
                write.get();
            }
        } catch (ExecutionException e) {
            for (Future<?> write : writes) {
                write.cancel(true);
            }
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("Writing partition file failed", cause);
        }
    }

    private static void append(File file, byte[] content) throws IOException {
        try (OutputStream output = new FileOutputStream(file, true)) {
            output.write(content);
        }
    }

    private static void delete(File file) {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            log.warn("Failed to delete: {}", file, e);
        }
    }

    private static final class PartitionFile {
        private final File target;
        private final Schema schema;
        private final List<File> spillFiles;
        private final PartitionWriter writer;
        private final Map<String, String> metadata;
        private final long spilledBytes;

        private PartitionFile(File target, Schema schema, List<File> spillFiles, PartitionWriter writer,
                              Map<String, String> metadata) {
            this.target = target;
            this.schema = schema;
            this.spillFiles = spillFiles;
            this.writer = writer;
            this.metadata = metadata;
            this.spilledBytes = spillFiles.stream().mapToLong(File::length).sum();
        }

        private long getSpilledBytes() {
            return spilledBytes;
        }
    }
}
