package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NodeWayArrowWriterTest {

    @Test
    void fill_nodes_rowsAndTags() {
        List<ArrowNodeOrWay> nodes = List.of(
                new ArrowNodeOrWay(1L, (short) 100, 200, 4.0, 73.0, List.of(new Tag("amenity", "restaurant"))),
                new ArrowNodeOrWay(2L, (short) 100, 200, 4.5, 73.5, Collections.emptyList()));

        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(ArrowSchemas.getNodesOrWaysSchema(false), allocator)) {
            NodeWayArrowWriter.fill(allocator, root, nodes, false);

            assertEquals(2, root.getRowCount());
            assertEquals(2L, ((BigIntVector) root.getVector("id")).get(1));
            List<?> tags = (List<?>) root.getVector("tags").getObject(0);
            assertEquals(1, tags.size());
            assertEquals(new Text("amenity"), ((Map<?, ?>) tags.get(0)).get("key"));
            assertEquals(new Text("restaurant"), ((Map<?, ?>) tags.get(0)).get("value"));
            assertTrue(root.getVector("tags").isNull(1));
        }
    }

    @Test
    void fill_wayWithNullH38Indexes_nullList() {
        ArrowNodeOrWay way = new ArrowNodeOrWay.Builder(3L, (short) 100, 200, 4.0, 73.0,
                List.of(new Tag("highway", "primary")))
                .pointIdxs(new long[]{1L, 2L})
                .lineStringWkb(new byte[]{0x01, 0x02})
                .bboxWkb(new byte[]{0x03, 0x04})
                .h38Indexes(null)
                .build();

        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(ArrowSchemas.getNodesOrWaysSchema(true), allocator)) {
            NodeWayArrowWriter.fill(allocator, root, List.of(way), true);

            assertEquals(1, root.getRowCount());
            assertEquals(List.of(1L, 2L), root.getVector("pointIdxs").getObject(0));
            assertTrue(root.getVector("h38Indexes").isNull(0));
            assertArrayEquals(new byte[]{0x01, 0x02}, (byte[]) root.getVector(ArrowSchemas.LINE_STRING_WKB).getObject(0));
        }
    }
}
