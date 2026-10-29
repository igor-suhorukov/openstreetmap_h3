package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BatchStatisticsTest {

    @TempDir
    Path tempDir;

    @Test
    void toJson_nodeBatches_rowsAndRangesOfIdH33H38() {
        BatchStatistics statistics = new BatchStatistics();
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(ArrowSchemas.getNodesOrWaysSchema(false), allocator)) {
            NodeWayArrowWriter.fill(allocator, root, List.of(node(7, -9, 300), node(3, -9, 100), node(5, -2, 200)),
                    false);
            statistics.add(root);
            NodeWayArrowWriter.fill(allocator, root, List.of(node(11, 4, 50)), false);
            statistics.add(root);
        }

        assertEquals("{\"version\":1,\"batches\":["
                + "{\"rows\":3,\"id\":[3,7],\"h33\":[-9,-2],\"h38\":[100,300]},"
                + "{\"rows\":1,\"id\":[11,11],\"h33\":[4,4],\"h38\":[50,50]}]}", statistics.toJson());
    }

    @Test
    void toJson_relationsAndNulls_onlyPresentColumnsNullWithoutValues() {
        BatchStatistics statistics = new BatchStatistics();
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot relations = VectorSchemaRoot.create(ArrowSchemas.getRelationSchema(), allocator);
             VectorSchemaRoot ids = VectorSchemaRoot.create(new Schema(List.of(
                     Field.nullable(ArrowSchemas.ID, new ArrowType.Int(64, true)))), allocator)) {
            RelationArrowWriter.fill(allocator, relations, List.of(new ArrowRelation(20L, Map.of()),
                    new ArrowRelation(25L, Map.of())));
            statistics.add(relations);
            BigIntVector id = (BigIntVector) ids.getVector(ArrowSchemas.ID);
            id.allocateNew(1);
            id.setNull(0);
            ids.setRowCount(1);
            statistics.add(ids);
        }

        assertEquals("{\"version\":1,\"batches\":[{\"rows\":2,\"id\":[20,25]},{\"rows\":1,\"id\":null}]}",
                statistics.toJson());
        assertEquals("{\"version\":1,\"batches\":[]}", new BatchStatistics().toJson());
    }

    @Test
    void addToFooter_fileWithoutStatistics_footerRewrittenRowsAndMetadataKept() throws IOException {
        File file = tempDir.resolve("nodes.arrow").toFile();
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(ArrowSchemas.getNodesOrWaysSchema(false), allocator);
             FileOutputStream output = new FileOutputStream(file);
             ArrowFileWriter writer = new ArrowFileWriter(root, null, output.getChannel(), Map.of("kept", "yes"))) {
            writer.start();
            NodeWayArrowWriter.fill(allocator, root, List.of(node(1, 5, 10), node(2, 5, 20)), false);
            writer.writeBatch();
            NodeWayArrowWriter.fill(allocator, root, List.of(node(3, 6, 30)), false);
            writer.writeBatch();
            writer.end();
        }

        try (BufferAllocator allocator = new RootAllocator()) {
            BatchStatistics.addToFooter(allocator, file);
        }

        try (BufferAllocator allocator = new RootAllocator();
             FileInputStream input = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(input.getChannel(), allocator,
                     CommonsCompressionFactory.INSTANCE)) {
            reader.getVectorSchemaRoot();
            assertEquals(Map.of("kept", "yes", BatchStatistics.METADATA_KEY, "{\"version\":1,\"batches\":["
                    + "{\"rows\":2,\"id\":[1,2],\"h33\":[5,5],\"h38\":[10,20]},"
                    + "{\"rows\":1,\"id\":[3,3],\"h33\":[6,6],\"h38\":[30,30]}]}"), reader.getMetaData());
            List<Long> ids = new ArrayList<>();
            while (reader.loadNextBatch()) {
                BigIntVector id = (BigIntVector) reader.getVectorSchemaRoot().getVector(ArrowSchemas.ID);
                for (int row = 0; row < reader.getVectorSchemaRoot().getRowCount(); row++) {
                    ids.add(id.get(row));
                }
            }
            assertEquals(List.of(1L, 2L, 3L), ids);
        }
    }

    private static ArrowNodeOrWay node(long id, int h33, int h38) {
        return new ArrowNodeOrWay(id, (short) h33, h38, 4.0, 73.0, List.of(new Tag("amenity", "cafe")));
    }
}
