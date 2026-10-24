package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

public class ArrowSchemas {

    private ArrowSchemas() {
    }

    public static Schema getNodesOrWaysSchema(boolean isWays) {
        FieldType mapType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
        FieldType keyType1 = new FieldType(false, new ArrowType.Utf8(), null, null);
        Map<String, String> idMetadata = null;
        Map<String, String> h33Metadata = null;
        Map<String, String> h38Metadata = null;
        FieldType idFieldType = new FieldType(false, new ArrowType.Int(64, true), null, idMetadata);
        FieldType h33FieldType = new FieldType(false, new ArrowType.Int(16, true), null, h33Metadata);
        FieldType h38FieldType = new FieldType(false, new ArrowType.Int(32, true), null, h38Metadata);
        List<Field> schemaFields = new ArrayList<>(Arrays.asList(
                new Field("id", idFieldType, null),
                new Field("h33", h33FieldType, null),
                new Field("h38", h38FieldType, null),
                new Field("latitude", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("longitude", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("tags", FieldType.nullable(new ArrowType.Map(true)),
                        Collections.singletonList(new Field(MapVector.KEY_NAME, mapType,
                                Arrays.asList(new Field(MapVector.KEY_NAME, keyType1, null),
                                        new Field(MapVector.VALUE_NAME, keyType1, null)))))
        ));
        if (isWays) {
            Map<String, String> h33CenterMetadata = null;
            FieldType h33CenterFieldType = new FieldType(false, new ArrowType.Int(16, true), null, h33CenterMetadata);
            schemaFields.addAll(Arrays.asList(
                    new Field("pointIdxs", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(new Field("point", FieldType.notNullable(new ArrowType.Int(64, true)), null)))
                    , new Field("h33Center", h33CenterFieldType, null)
                    , new Field("closed", FieldType.notNullable(new ArrowType.Bool()), null)
                    , new Field("building", FieldType.notNullable(new ArrowType.Bool()), null)
                    , new Field("highway", FieldType.notNullable(new ArrowType.Bool()), null)
                    , new Field("scale", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null)
                    , new Field("lineStringWkb", FieldType.notNullable(new ArrowType.Binary()), null)
                    , new Field("bboxWkb", FieldType.notNullable(new ArrowType.Binary()), null)
                    , new Field("h38Indexes", FieldType.nullable(new ArrowType.List()), Collections.singletonList(new Field("h38Idx", FieldType.notNullable(new ArrowType.Int(32, true)), null)))
                    , new Field("bboxMinX", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMaxX", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMinY", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMaxY", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
            ));
        }
        return new Schema(schemaFields);
    }

    public static Schema getRelationSchema() {
        FieldType mapType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
        FieldType keyType1 = new FieldType(false, new ArrowType.Utf8(), null, null);
        Map<String, String> idMetadata = null;
        FieldType idFieldType = new FieldType(false, new ArrowType.Int(64, true), null, idMetadata);
        List<Field> schemaFields = new ArrayList<>(Arrays.asList(
                new Field("id", idFieldType, null),
                new Field("tags", FieldType.nullable(new ArrowType.Map(true)),
                        Collections.singletonList(new Field(MapVector.KEY_NAME, mapType,
                                Arrays.asList(new Field(MapVector.KEY_NAME, keyType1, null),
                                        new Field(MapVector.VALUE_NAME, keyType1, null)))))
                , new Field("memberId", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(
                        new Field("memberId", FieldType.notNullable(new ArrowType.Int(64, true)), null)
                ))
                , new Field("memberType", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(
                        new Field("memberType", FieldType.notNullable(new ArrowType.Int(8, true)), null)
                ))
                , new Field("memberRole", FieldType.notNullable(new ArrowType.List()), Collections.singletonList(
                        new Field("memberRole", FieldType.notNullable(new ArrowType.Utf8()), null)
                ))
        ));
        return new Schema(schemaFields);
    }
}
