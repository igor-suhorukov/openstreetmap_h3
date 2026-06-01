package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.ArrowFormat;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelation;
import com.github.isuhorukov.osm.pgsnapshot.model.ArrowRelationMember;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RelationArrowWriter extends ArrowBatchWriter {

    public RelationArrowWriter(File resultDir, ArrowFormat arrowFormat) {
        super(resultDir, arrowFormat);
    }

    public void write(ArrayList<ArrowRelation> arrowRelations, Long blockNumber) {
        if (arrowRelations == null || arrowRelations.isEmpty()) {
            return;
        }
        Schema schema = ArrowSchemas.getRelationSchema();
        try (BufferAllocator allocator = new RootAllocator()) {
            try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector idVector = (BigIntVector) vectorSchemaRoot.getVector("id");
                MapVector mapVector = (MapVector) vectorSchemaRoot.getVector("tags");
                ListVector memberIdVector = (ListVector) vectorSchemaRoot.getVector("memberId");
                ListVector memberTypeVector = (ListVector) vectorSchemaRoot.getVector("memberType");
                ListVector memberRoleVector = (ListVector) vectorSchemaRoot.getVector("memberRole");

                UnionListWriter memberIdVectorWriter = memberIdVector.getWriter();
                UnionListWriter memberTypeVectorWriter = memberTypeVector.getWriter();
                UnionListWriter memberRoleVectorWriter = memberRoleVector.getWriter();

                memberIdVectorWriter.allocate();
                memberTypeVectorWriter.allocate();
                memberRoleVectorWriter.allocate();

                idVector.allocateNew(arrowRelations.size());
                mapVector.allocateNew();
                UnionMapWriter mapWriter = mapVector.getWriter();

                for (int idx = 0; idx < arrowRelations.size(); idx++) {
                    ArrowRelation arrowRelation = arrowRelations.get(idx);
                    List<ArrowRelationMember> relationMembers = arrowRelation.getRelationMembers();

                    idVector.set(idx, arrowRelation.getId());
                    writeTagsToArrow(allocator, mapWriter, idx, arrowRelation.getTags());

                    memberIdVectorWriter.setPosition(idx);
                    memberTypeVectorWriter.setPosition(idx);
                    memberRoleVectorWriter.setPosition(idx);
                    if (!relationMembers.isEmpty()) {
                        memberIdVectorWriter.startList();
                        memberTypeVectorWriter.startList();
                        memberRoleVectorWriter.startList();
                        for (int pidx = 0; pidx < relationMembers.size(); pidx++) {
                            ArrowRelationMember arrowRelationMember = relationMembers.get(pidx);
                            memberIdVectorWriter.bigInt().writeBigInt(arrowRelationMember.getMemberId());
                            memberTypeVectorWriter.tinyInt().writeTinyInt((byte) arrowRelationMember.getMemberType());
                            byte[] roleBytes = arrowRelationMember.getMemberRole().getBytes(StandardCharsets.UTF_8);
                            try (ArrowBuf roleBytesBuf = allocator.buffer(roleBytes.length)) {
                                roleBytesBuf.writeBytes(roleBytes);
                                memberRoleVectorWriter.writeVarChar(0, roleBytes.length, roleBytesBuf);
                            }
                        }
                        memberIdVectorWriter.endList();
                        memberTypeVectorWriter.endList();
                        memberRoleVectorWriter.endList();
                        memberIdVector.setLastSet(idx);
                        memberTypeVector.setLastSet(idx);
                        memberRoleVector.setLastSet(idx);
                    } else {
                        memberIdVectorWriter.setAddVectorAsNullable(true);
                        memberTypeVectorWriter.setAddVectorAsNullable(true);
                        memberRoleVectorWriter.setAddVectorAsNullable(true);
                    }
                }
                memberIdVector.setValueCount(arrowRelations.size());
                memberTypeVector.setValueCount(arrowRelations.size());
                memberRoleVector.setValueCount(arrowRelations.size());
                mapWriter.setValueCount(arrowRelations.size());
                vectorSchemaRoot.setRowCount(arrowRelations.size());
                String fileName = String.format("%s/%08d", ResultLayout.RELATIONS_DIR, blockNumber);
                dispatch(allocator, vectorSchemaRoot, fileName, blockNumber);
            }
        } catch (Exception e) {
            log.error("block {}", blockNumber, e);
            System.exit(-1);
        }
    }
}
