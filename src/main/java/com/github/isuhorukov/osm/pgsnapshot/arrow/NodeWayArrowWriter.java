package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowNodeOrWay;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.util.ArrayList;

public class NodeWayArrowWriter extends ArrowBatchWriter {

    public NodeWayArrowWriter(File resultDir, ArrowFormat arrowFormat) {
        super(resultDir, arrowFormat);
    }

    public void write(ArrayList<ArrowNodeOrWay> arrowNodeOrWays, Long blockNumber) {
        if (arrowNodeOrWays == null || arrowNodeOrWays.isEmpty()) {
            return;
        }
        boolean isWays = arrowNodeOrWays.stream().anyMatch(arrowNodeOrWay -> arrowNodeOrWay.getPointIdxs() != null);
        Schema schema = ArrowSchemas.getNodesOrWaysSchema(isWays);

        try (BufferAllocator allocator = new RootAllocator()) {
            try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector idVector = (BigIntVector) vectorSchemaRoot.getVector("id");
                SmallIntVector h33Vector = (SmallIntVector) vectorSchemaRoot.getVector("h33");
                IntVector h38Vector = (IntVector) vectorSchemaRoot.getVector("h38");
                Float8Vector latitudeVector = (Float8Vector) vectorSchemaRoot.getVector("latitude");
                Float8Vector longitudeVector = (Float8Vector) vectorSchemaRoot.getVector("longitude");
                MapVector mapVector = (MapVector) vectorSchemaRoot.getVector("tags");
                idVector.allocateNew(arrowNodeOrWays.size());
                h33Vector.allocateNew(arrowNodeOrWays.size());
                h38Vector.allocateNew(arrowNodeOrWays.size());
                latitudeVector.allocateNew(arrowNodeOrWays.size());
                longitudeVector.allocateNew(arrowNodeOrWays.size());
                UnionMapWriter mapWriter = mapVector.getWriter();
                mapVector.allocateNew();

                ListVector pointIdxsVector = null;
                SmallIntVector h33CenterVector = null;
                BitVector isClosed = null;
                BitVector building = null;
                BitVector highway = null;
                Float4Vector scale = null;
                VarBinaryVector lineStringWkb = null;
                VarBinaryVector bboxWkb = null;
                ListVector h38IndexesVector = null;
                Float8Vector bboxMinX = null;
                Float8Vector bboxMaxX = null;
                Float8Vector bboxMinY = null;
                Float8Vector bboxMaxY = null;
                UnionListWriter pointIdxsVectorWriter = null;
                UnionListWriter h38IndexesVectorWriter = null;

                if (isWays) {
                    pointIdxsVector = (ListVector) vectorSchemaRoot.getVector("pointIdxs");
                    h33CenterVector = (SmallIntVector) vectorSchemaRoot.getVector("h33Center");
                    isClosed = (BitVector) vectorSchemaRoot.getVector("closed");
                    building = (BitVector) vectorSchemaRoot.getVector("building");
                    highway = (BitVector) vectorSchemaRoot.getVector("highway");
                    scale = (Float4Vector) vectorSchemaRoot.getVector("scale");
                    lineStringWkb = (VarBinaryVector) vectorSchemaRoot.getVector("lineStringWkb");
                    bboxWkb = (VarBinaryVector) vectorSchemaRoot.getVector("bboxWkb");
                    h38IndexesVector = (ListVector) vectorSchemaRoot.getVector("h38Indexes");
                    bboxMinX = (Float8Vector) vectorSchemaRoot.getVector("bboxMinX");
                    bboxMaxX = (Float8Vector) vectorSchemaRoot.getVector("bboxMaxX");
                    bboxMinY = (Float8Vector) vectorSchemaRoot.getVector("bboxMinY");
                    bboxMaxY = (Float8Vector) vectorSchemaRoot.getVector("bboxMaxY");
                    pointIdxsVectorWriter = pointIdxsVector.getWriter();
                    pointIdxsVectorWriter.allocate();
                    h38IndexesVectorWriter = h38IndexesVector.getWriter();
                    h38IndexesVectorWriter.allocate();
                    h33CenterVector.allocateNew(arrowNodeOrWays.size());
                    isClosed.allocateNew(arrowNodeOrWays.size());
                    building.allocateNew(arrowNodeOrWays.size());
                    highway.allocateNew(arrowNodeOrWays.size());
                    scale.allocateNew(arrowNodeOrWays.size());
                    lineStringWkb.allocateNew(1024L * 1024 * 10, arrowNodeOrWays.size());
                    bboxWkb.allocateNew(8000L * 94, arrowNodeOrWays.size());
                    bboxMinX.allocateNew(arrowNodeOrWays.size());
                    bboxMaxX.allocateNew(arrowNodeOrWays.size());
                    bboxMinY.allocateNew(arrowNodeOrWays.size());
                    bboxMaxY.allocateNew(arrowNodeOrWays.size());
                }

                for (int idx = 0; idx < arrowNodeOrWays.size(); idx++) {
                    ArrowNodeOrWay arrowNodeOrWay = arrowNodeOrWays.get(idx);
                    long arrowNodeOrWayId = arrowNodeOrWay.getId();
                    idVector.set(idx, arrowNodeOrWayId);
                    h33Vector.set(idx, arrowNodeOrWay.getH33());
                    h38Vector.set(idx, arrowNodeOrWay.getH38());
                    latitudeVector.set(idx, arrowNodeOrWay.getLatitude());
                    longitudeVector.set(idx, arrowNodeOrWay.getLongitude());
                    writeTagsToArrow(allocator, mapWriter, idx, arrowNodeOrWay.getTags());
                    if (isWays) {
                        pointIdxsVectorWriter.setPosition(idx);
                        pointIdxsVectorWriter.startList();
                        for (int pidx = 0; pidx < arrowNodeOrWay.getPointIdxs().length; pidx++) {
                            pointIdxsVectorWriter.bigInt().writeBigInt(arrowNodeOrWay.getPointIdxs()[pidx]);
                        }
                        pointIdxsVectorWriter.setValueCount(arrowNodeOrWay.getPointIdxs().length);
                        pointIdxsVectorWriter.endList();
                        pointIdxsVector.setLastSet(idx);
                        if (arrowNodeOrWay.getH38Indexes() != null && arrowNodeOrWay.getH38Indexes().length > 0) {
                            h38IndexesVectorWriter.setPosition(idx);
                            h38IndexesVectorWriter.startList();
                            for (int pidx = 0; pidx < arrowNodeOrWay.getH38Indexes().length; pidx++) {
                                h38IndexesVectorWriter.integer().writeInt(arrowNodeOrWay.getH38Indexes()[pidx]);
                            }
                            h38IndexesVectorWriter.setValueCount(arrowNodeOrWay.getH38Indexes().length);
                            h38IndexesVectorWriter.endList();
                            h38IndexesVector.setLastSet(idx);
                        } else {
                            h38IndexesVectorWriter.setPosition(idx);
                            h38IndexesVectorWriter.setAddVectorAsNullable(true);
                        }
                        h33CenterVector.set(idx, arrowNodeOrWay.getH33Center());
                        isClosed.set(idx, arrowNodeOrWay.isClosed() ? 1 : 0);
                        building.set(idx, arrowNodeOrWay.isBuilding() ? 1 : 0);
                        highway.set(idx, arrowNodeOrWay.isHighway() ? 1 : 0);
                        scale.set(idx, arrowNodeOrWay.getScaleDim());
                        lineStringWkb.set(idx, arrowNodeOrWay.getLineStringWkb());
                        bboxWkb.set(idx, arrowNodeOrWay.getBboxWkb());
                        bboxMinX.set(idx, arrowNodeOrWay.getBboxMinX());
                        bboxMaxX.set(idx, arrowNodeOrWay.getBboxMaxX());
                        bboxMinY.set(idx, arrowNodeOrWay.getBboxMinY());
                        bboxMaxY.set(idx, arrowNodeOrWay.getBboxMaxY());
                    }
                }
                mapWriter.setValueCount(arrowNodeOrWays.size());
                if (isWays) {
                    pointIdxsVector.setValueCount(arrowNodeOrWays.size());
                    h38IndexesVector.setValueCount(arrowNodeOrWays.size());
                }
                vectorSchemaRoot.setRowCount(arrowNodeOrWays.size());

                String fileName = String.format("%s/%08d", isWays ? ResultLayout.WAYS_DIR : ResultLayout.NODES_DIR, blockNumber);
                dispatch(allocator, vectorSchemaRoot, fileName, blockNumber);
            }
        } catch (Exception e) {
            log.error("block {}", blockNumber, e);
            System.exit(-1);
        }
    }
}
