package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface PartitionWriter {

    String fileExtension();

    void write(BufferAllocator allocator, ArrowReader reader, File target, List<String> orderBy) throws IOException;

    default void write(BufferAllocator allocator, ArrowReader reader, File target) throws IOException {
        write(allocator, reader, target, List.of());
    }
}
