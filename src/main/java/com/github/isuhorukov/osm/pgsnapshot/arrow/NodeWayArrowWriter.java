package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.util.List;

public class NodeWayArrowWriter extends ArrowBatchWriter {

    public NodeWayArrowWriter(File resultDir, ArrowFormat arrowFormat) {
        super(resultDir, arrowFormat);
    }

    public void write(List<ArrowNodeOrWay> arrowNodeOrWays, Long blockNumber) {
        if (arrowNodeOrWays == null || arrowNodeOrWays.isEmpty()) {
            return;
        }
        boolean isWays = arrowNodeOrWays.stream().anyMatch(item -> item.getPointIdxs() != null);
        Schema schema = ArrowSchemas.getNodesOrWaysSchema(isWays);
        String fileName = String.format("%s/%08d",
                isWays ? ResultLayout.WAYS_DIR : ResultLayout.NODES_DIR, blockNumber);
        writeBlock(blockNumber, schema, fileName, (allocator, root) -> {
            BigIntVector idVector = (BigIntVector) root.getVector("id");
            SmallIntVector h33Vector = (SmallIntVector) root.getVector("h33");
            IntVector h38Vector = (IntVector) root.getVector("h38");
            Float8Vector latitudeVector = (Float8Vector) root.getVector("latitude");
            Float8Vector longitudeVector = (Float8Vector) root.getVector("longitude");
            MapVector mapVector = (MapVector) root.getVector("tags");
            int size = arrowNodeOrWays.size();
            idVector.allocateNew(size);
            h33Vector.allocateNew(size);
            h38Vector.allocateNew(size);
            latitudeVector.allocateNew(size);
            longitudeVector.allocateNew(size);
            mapVector.allocateNew();
            UnionMapWriter mapWriter = mapVector.getWriter();
            WayColumnVectors wayVectors = isWays ? new WayColumnVectors(root, size) : null;

            for (int idx = 0; idx < size; idx++) {
                ArrowNodeOrWay item = arrowNodeOrWays.get(idx);
                idVector.set(idx, item.getId());
                h33Vector.set(idx, item.getH33());
                h38Vector.set(idx, item.getH38());
                latitudeVector.set(idx, item.getLatitude());
                longitudeVector.set(idx, item.getLongitude());
                writeTagsToArrow(allocator, mapWriter, idx, item.getTags());
                if (wayVectors != null) {
                    wayVectors.writeElement(idx, item);
                }
            }
            mapWriter.setValueCount(size);
            if (wayVectors != null) {
                wayVectors.finalizeValueCounts(size);
            }
            root.setRowCount(size);
        });
    }

    private static final class WayColumnVectors {
        private final ListVector pointIdxsVector;
        private final SmallIntVector h33CenterVector;
        private final BitVector isClosed;
        private final BitVector building;
        private final BitVector highway;
        private final Float4Vector scale;
        private final VarBinaryVector lineStringWkb;
        private final VarBinaryVector bboxWkb;
        private final ListVector h38IndexesVector;
        private final Float8Vector bboxMinX;
        private final Float8Vector bboxMaxX;
        private final Float8Vector bboxMinY;
        private final Float8Vector bboxMaxY;
        private final UnionListWriter pointIdxsVectorWriter;
        private final UnionListWriter h38IndexesVectorWriter;

        WayColumnVectors(VectorSchemaRoot root, int size) {
            pointIdxsVector = (ListVector) root.getVector("pointIdxs");
            h33CenterVector = (SmallIntVector) root.getVector("h33Center");
            isClosed = (BitVector) root.getVector("closed");
            building = (BitVector) root.getVector("building");
            highway = (BitVector) root.getVector("highway");
            scale = (Float4Vector) root.getVector("scale");
            lineStringWkb = (VarBinaryVector) root.getVector("lineStringWkb");
            bboxWkb = (VarBinaryVector) root.getVector("bboxWkb");
            h38IndexesVector = (ListVector) root.getVector("h38Indexes");
            bboxMinX = (Float8Vector) root.getVector("bboxMinX");
            bboxMaxX = (Float8Vector) root.getVector("bboxMaxX");
            bboxMinY = (Float8Vector) root.getVector("bboxMinY");
            bboxMaxY = (Float8Vector) root.getVector("bboxMaxY");
            pointIdxsVectorWriter = pointIdxsVector.getWriter();
            pointIdxsVectorWriter.allocate();
            h38IndexesVectorWriter = h38IndexesVector.getWriter();
            h38IndexesVectorWriter.allocate();
            h33CenterVector.allocateNew(size);
            isClosed.allocateNew(size);
            building.allocateNew(size);
            highway.allocateNew(size);
            scale.allocateNew(size);
            lineStringWkb.allocateNew(1024L * 1024 * 10, size);
            bboxWkb.allocateNew(8000L * 94, size);
            bboxMinX.allocateNew(size);
            bboxMaxX.allocateNew(size);
            bboxMinY.allocateNew(size);
            bboxMaxY.allocateNew(size);
        }

        void writeElement(int idx, ArrowNodeOrWay item) {
            writePointIdxs(idx, item);
            writeH38Indexes(idx, item);
            h33CenterVector.set(idx, item.getH33Center());
            isClosed.set(idx, item.isClosed() ? 1 : 0);
            building.set(idx, item.isBuilding() ? 1 : 0);
            highway.set(idx, item.isHighway() ? 1 : 0);
            scale.set(idx, item.getScaleDim());
            lineStringWkb.set(idx, item.getLineStringWkb());
            bboxWkb.set(idx, item.getBboxWkb());
            bboxMinX.set(idx, item.getBboxMinX());
            bboxMaxX.set(idx, item.getBboxMaxX());
            bboxMinY.set(idx, item.getBboxMinY());
            bboxMaxY.set(idx, item.getBboxMaxY());
        }

        private void writePointIdxs(int idx, ArrowNodeOrWay item) {
            pointIdxsVectorWriter.setPosition(idx);
            pointIdxsVectorWriter.startList();
            for (long pointIdx : item.getPointIdxs()) {
                pointIdxsVectorWriter.bigInt().writeBigInt(pointIdx);
            }
            pointIdxsVectorWriter.setValueCount(item.getPointIdxs().length);
            pointIdxsVectorWriter.endList();
            pointIdxsVector.setLastSet(idx);
        }

        private void writeH38Indexes(int idx, ArrowNodeOrWay item) {
            if (item.getH38Indexes() != null && item.getH38Indexes().length > 0) {
                h38IndexesVectorWriter.setPosition(idx);
                h38IndexesVectorWriter.startList();
                for (int h38Idx : item.getH38Indexes()) {
                    h38IndexesVectorWriter.integer().writeInt(h38Idx);
                }
                h38IndexesVectorWriter.setValueCount(item.getH38Indexes().length);
                h38IndexesVectorWriter.endList();
                h38IndexesVector.setLastSet(idx);
            } else {
                h38IndexesVectorWriter.setPosition(idx);
                h38IndexesVectorWriter.setAddVectorAsNullable(true);
            }
        }

        void finalizeValueCounts(int size) {
            pointIdxsVector.setValueCount(size);
            h38IndexesVector.setValueCount(size);
        }
    }
}
