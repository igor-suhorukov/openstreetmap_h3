package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.admin.AdminColumns;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

public class ArrowSchemas {

    public static final String LINE_STRING_WKB = "lineStringWkb";
    public static final String ID = "id";
    public static final String H33 = "h33";
    public static final String WKB = "wkb";
    public static final String TAGS = "tags";

    private ArrowSchemas() {
    }

    public static Schema getNodesOrWaysSchema(boolean isWays) {
        return getNodesOrWaysSchema(isWays, AdminColumns.NONE);
    }

    public static Schema getNodesOrWaysSchema(boolean isWays, AdminColumns adminColumns) {
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
                tagsField()
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
                    , new Field(LINE_STRING_WKB, FieldType.notNullable(new ArrowType.Binary()), null)
                    , new Field("bboxWkb", FieldType.notNullable(new ArrowType.Binary()), null)
                    , new Field("h38Indexes", FieldType.nullable(new ArrowType.List()), Collections.singletonList(new Field("h38Idx", FieldType.notNullable(new ArrowType.Int(32, true)), null)))
                    , new Field("bboxMinX", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMaxX", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMinY", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
                    , new Field("bboxMaxY", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
            ));
        }
        schemaFields.addAll(adminFields(adminColumns));
        return new Schema(schemaFields);
    }

    public static Schema getMultipolygonSchema() {
        return getMultipolygonSchema(AdminColumns.NONE);
    }

    public static Schema getMultipolygonSchema(AdminColumns adminColumns) {
        List<Field> fields = new ArrayList<>(Arrays.asList(
                new Field(ID, FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field(H33, FieldType.notNullable(new ArrowType.Int(16, true)), null),
                new Field(WKB, FieldType.notNullable(new ArrowType.Binary()), null),
                tagsField()));
        fields.addAll(adminFields(adminColumns));
        return new Schema(fields);
    }

    static List<Field> adminFields(AdminColumns adminColumns) {
        List<Field> fields = new ArrayList<>();
        if (adminColumns.isEnabled()) {
            fields.add(listField(AdminColumns.UNITS_COLUMN, "unit_id", new ArrowType.Int(64, true), true));
        }
        if (adminColumns.hasShares()) {
            fields.add(listField(AdminColumns.SHARES_COLUMN, "share",
                    new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), true));
        }
        return fields;
    }

    static Field listField(String name, String elementName, ArrowType elementType, boolean nullable) {
        return new Field(name, nullable ? FieldType.nullable(new ArrowType.List()) : FieldType.notNullable(new ArrowType.List()),
                Collections.singletonList(new Field(elementName, FieldType.notNullable(elementType), null)));
    }

    public static Schema getRelationSchema() {
        Map<String, String> idMetadata = null;
        FieldType idFieldType = new FieldType(false, new ArrowType.Int(64, true), null, idMetadata);
        List<Field> schemaFields = new ArrayList<>(Arrays.asList(
                new Field("id", idFieldType, null),
                tagsField()
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

    private static Field tagsField() {
        return mapField(TAGS);
    }

    static Field mapField(String name) {
        FieldType entryType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
        FieldType stringType = new FieldType(false, new ArrowType.Utf8(), null, null);
        return new Field(name, FieldType.nullable(new ArrowType.Map(true)),
                Collections.singletonList(new Field(MapVector.KEY_NAME, entryType,
                        Arrays.asList(new Field(MapVector.KEY_NAME, stringType, null),
                                new Field(MapVector.VALUE_NAME, stringType, null)))));
    }
}
