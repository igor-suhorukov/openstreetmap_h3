package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelationMember;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RelationArrowWriterTest {

    @Test
    void fill_relations_membersAndNullLists() {
        ArrowRelation withMembers = new ArrowRelation(2L, Map.of("natural", "water"));
        withMembers.getRelationMembers().add(new ArrowRelationMember(5L, 'W', "outer"));
        withMembers.getRelationMembers().add(new ArrowRelationMember(6L, 'N', "label"));
        ArrowRelation withoutMembers = new ArrowRelation(3L, Collections.emptyMap());

        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(ArrowSchemas.getRelationSchema(), allocator)) {
            RelationArrowWriter.fill(allocator, root, List.of(withMembers, withoutMembers));

            assertEquals(2, root.getRowCount());
            assertEquals(List.of(5L, 6L), root.getVector("memberId").getObject(0));
            assertEquals(List.of((byte) 'W', (byte) 'N'), root.getVector("memberType").getObject(0));
            assertEquals(List.of(new Text("outer"), new Text("label")), root.getVector("memberRole").getObject(0));
            assertTrue(root.getVector("memberId").isNull(1));
            assertTrue(root.getVector("tags").isNull(1));
        }
    }
}
