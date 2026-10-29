package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.model.ArrowMultipolygon;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;

import java.util.List;

final class MultipolygonArrowWriter {

    private MultipolygonArrowWriter() {
    }

    static void fill(BufferAllocator allocator, VectorSchemaRoot root, List<ArrowMultipolygon> multipolygons) {
        BigIntVector idVector = (BigIntVector) root.getVector(ArrowSchemas.ID);
        SmallIntVector h33Vector = (SmallIntVector) root.getVector(ArrowSchemas.H33);
        VarBinaryVector wkbVector = (VarBinaryVector) root.getVector(ArrowSchemas.WKB);
        MapVector tagsVector = (MapVector) root.getVector(ArrowSchemas.TAGS);
        root.allocateNew();
        UnionMapWriter mapWriter = tagsVector.getWriter();
        AdminArrowColumns adminColumns = AdminArrowColumns.of(root);
        for (int idx = 0; idx < multipolygons.size(); idx++) {
            ArrowMultipolygon multipolygon = multipolygons.get(idx);
            idVector.setSafe(idx, multipolygon.getId());
            h33Vector.setSafe(idx, multipolygon.getH33());
            wkbVector.setSafe(idx, multipolygon.getWkb());
            ArrowTags.write(allocator, mapWriter, idx, multipolygon.getTags());
            if (adminColumns != null) {
                adminColumns.write(idx, multipolygon.getAdminBinding());
            }
        }
        mapWriter.setValueCount(multipolygons.size());
        if (adminColumns != null) {
            adminColumns.setValueCount(multipolygons.size());
        }
        root.setRowCount(multipolygons.size());
    }
}
