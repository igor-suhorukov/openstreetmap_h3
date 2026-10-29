package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public final class CellBatchReader extends ArrowReader {

    public static final int MAX_BATCH_ROWS = 1 << 16;

    private final Schema schema;
    private final Iterator<File> cellFiles;
    private final int maxBatchRows;
    private ArrowReader cell;
    private boolean pending;
    private long bytesRead;

    public CellBatchReader(BufferAllocator allocator, Schema schema, List<File> cellFiles, int maxBatchRows) {
        super(allocator, CommonsCompressionFactory.INSTANCE);
        this.schema = schema;
        this.cellFiles = cellFiles.iterator();
        this.maxBatchRows = maxBatchRows;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        VectorSchemaRoot batch = getVectorSchemaRoot();
        batch.clear();
        batch.allocateNew();
        batch.setRowCount(0);
        while (true) {
            if (cell == null) {
                if (!cellFiles.hasNext()) {
                    return batch.getRowCount() > 0;
                }
                File cellFile = cellFiles.next();
                bytesRead += cellFile.length();
                cell = ArrowSpill.reader(allocator, schema, List.of(cellFile));
            }
            if (!pending && !cell.loadNextBatch()) {
                cell.close();
                cell = null;
                if (batch.getRowCount() > 0) {
                    return true;
                }
                continue;
            }
            VectorSchemaRoot chunk = cell.getVectorSchemaRoot();
            if (batch.getRowCount() > 0 && batch.getRowCount() + chunk.getRowCount() > maxBatchRows) {
                pending = true;
                return true;
            }
            VectorSchemaRootAppender.append(false, batch, chunk);
            pending = false;
        }
    }

    @Override
    public long bytesRead() {
        return bytesRead;
    }

    @Override
    protected void closeReadSource() throws IOException {
        if (cell != null) {
            cell.close();
            cell = null;
        }
    }

    @Override
    protected Schema readSchema() {
        return schema;
    }
}
