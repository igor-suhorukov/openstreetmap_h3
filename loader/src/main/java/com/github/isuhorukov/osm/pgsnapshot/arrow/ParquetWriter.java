package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface ParquetWriter extends Closeable {

    void write(BufferAllocator allocator, ArrowReader reader, File target) throws IOException;
}
