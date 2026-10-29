package com.github.isuhorukov.osm.pgsnapshot.arrow;

import com.github.isuhorukov.osm.pgsnapshot.admin.AdminDirectory;
import com.github.isuhorukov.osm.pgsnapshot.admin.AdminUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class AdminDirectoryArrow {

    public static final String UNITS_FILE = "admin_units";
    public static final String REGIONS_FILE = "admin_regions";
    public static final String GEOM = "geom";
    private static final long MAX_BATCH_BYTES = 64L << 20;
    private static final String ID = "id";
    private static final String ADMIN_LEVEL = "admin_level";
    private static final String PARENT_ID = "parent_id";
    private static final String PARENT_SOURCE = "parent_source";
    private static final String PARENT_SHARE = "parent_share";
    private static final String PATH = "path";
    private static final String DEPTH = "depth";
    private static final String CHILDREN = "children";
    private static final String NAME = "name";
    private static final String NAMES = "names";
    private static final String H3_3 = "h3_3";
    private static final String AREA_M2 = "area_m2";
    private static final String OWN_AREA_M2 = "own_area_m2";
    private static final String UNIT_ID = "unit_id";
    private static final String PART_NO = "part_no";

    private AdminDirectoryArrow() {
    }

    public static void write(AdminDirectory directory, PartitionWriter writer, File targetDirectory,
                             Map<String, String> metadata) throws IOException {
        writeUnits(directory, writer, targetDirectory, metadata);
        writeRegions(directory, writer, targetDirectory, metadata);
    }

    public static void writeUnits(AdminDirectory directory, PartitionWriter writer, File targetDirectory,
                                  Map<String, String> metadata) throws IOException {
        createDirectory(targetDirectory);
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = new UnitsReader(allocator, directory)) {
            writer.write(allocator, reader, new File(targetDirectory, UNITS_FILE + writer.fileExtension()),
                    List.of(), metadata);
        }
    }

    public static void writeRegions(AdminDirectory directory, PartitionWriter writer, File targetDirectory,
                                    Map<String, String> metadata) throws IOException {
        createDirectory(targetDirectory);
        try (BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = new RegionsReader(allocator, directory)) {
            writer.write(allocator, reader, new File(targetDirectory, REGIONS_FILE + writer.fileExtension()),
                    List.of(), metadata);
        }
    }

    private static synchronized void createDirectory(File directory) throws IOException {
        if (!directory.isDirectory() && !directory.mkdirs()) {
            throw new IOException("Can't create directory " + directory);
        }
    }

    static Schema unitsSchema() {
        return new Schema(Arrays.asList(
                new Field(ID, FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field(ADMIN_LEVEL, FieldType.notNullable(new ArrowType.Int(16, true)), null),
                new Field(PARENT_ID, FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field(PARENT_SOURCE, FieldType.nullable(new ArrowType.Utf8()), null),
                new Field(PARENT_SHARE, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                        null),
                ArrowSchemas.listField(PATH, UNIT_ID, new ArrowType.Int(64, true), false),
                new Field(DEPTH, FieldType.notNullable(new ArrowType.Int(16, true)), null),
                new Field(CHILDREN, FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field(NAME, FieldType.nullable(new ArrowType.Utf8()), null),
                ArrowSchemas.mapField(NAMES),
                ArrowSchemas.mapField(ArrowSchemas.TAGS),
                ArrowSchemas.listField(H3_3, "cell", new ArrowType.Int(16, true), false),
                new Field(GEOM, FieldType.nullable(new ArrowType.Binary()), null),
                new Field(AREA_M2, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field(OWN_AREA_M2, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        null)));
    }

    static Schema regionsSchema() {
        return new Schema(Arrays.asList(
                new Field(UNIT_ID, FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field(PART_NO, FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field(ADMIN_LEVEL, FieldType.notNullable(new ArrowType.Int(16, true)), null),
                new Field(GEOM, FieldType.notNullable(new ArrowType.Binary()), null)));
    }

    private abstract static class BatchReader extends ArrowReader {
        private final Schema schema;
        private final int rows;
        private int next;

        BatchReader(BufferAllocator allocator, Schema schema, int rows) {
            super(allocator);
            this.schema = schema;
            this.rows = rows;
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            if (next >= rows) {
                return false;
            }
            VectorSchemaRoot root = getVectorSchemaRoot();
            root.clear();
            root.allocateNew();
            int count = 0;
            long bytes = 0;
            WKBWriter wkbWriter = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
            while (next < rows && count < CellBatchReader.MAX_BATCH_ROWS && bytes < MAX_BATCH_BYTES) {
                bytes += fill(root, count++, next++, wkbWriter);
            }
            finish(root, count);
            root.setRowCount(count);
            return true;
        }

        abstract long fill(VectorSchemaRoot root, int index, int row, WKBWriter wkbWriter);

        void finish(VectorSchemaRoot root, int count) {
        }

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        protected void closeReadSource() {
        }

        @Override
        protected Schema readSchema() {
            return schema;
        }
    }

    private static final class UnitsReader extends BatchReader {
        private final AdminDirectory directory;
        private UnionMapWriter namesWriter;
        private UnionMapWriter tagsWriter;

        private UnitsReader(BufferAllocator allocator, AdminDirectory directory) {
            super(allocator, unitsSchema(), directory.getUnits().size());
            this.directory = directory;
        }

        @Override
        long fill(VectorSchemaRoot root, int index, int row, WKBWriter wkbWriter) {
            if (index == 0) {
                namesWriter = ((MapVector) root.getVector(NAMES)).getWriter();
                tagsWriter = ((MapVector) root.getVector(ArrowSchemas.TAGS)).getWriter();
            }
            AdminUnit unit = directory.getUnits().get(row);
            ((BigIntVector) root.getVector(ID)).setSafe(index, unit.getId());
            ((SmallIntVector) root.getVector(ADMIN_LEVEL)).setSafe(index, unit.getLevel());
            AdminUnit parent = directory.parent(unit);
            if (parent != null) {
                ((BigIntVector) root.getVector(PARENT_ID)).setSafe(index, parent.getId());
                ((VarCharVector) root.getVector(PARENT_SOURCE)).setSafe(index,
                        String.valueOf(unit.getParentSource()).getBytes(StandardCharsets.UTF_8));
            }
            Float parentShare = directory.parentShare(unit);
            if (parentShare != null) {
                ((Float4Vector) root.getVector(PARENT_SHARE)).setSafe(index, parentShare);
            }
            ListVector path = (ListVector) root.getVector(PATH);
            long[] pathIds = directory.pathIds(unit);
            int pathStart = path.startNewValue(index);
            for (int i = 0; i < pathIds.length; i++) {
                ((BigIntVector) path.getDataVector()).setSafe(pathStart + i, pathIds[i]);
            }
            path.endValue(index, pathIds.length);
            ((SmallIntVector) root.getVector(DEPTH)).setSafe(index, unit.getDepth());
            ((IntVector) root.getVector(CHILDREN)).setSafe(index, unit.getChildren());
            String name = directory.name(unit);
            if (name != null) {
                ((VarCharVector) root.getVector(NAME)).setSafe(index, name.getBytes(StandardCharsets.UTF_8));
            }
            ArrowTags.write(allocator, namesWriter, index, directory.names(unit));
            ArrowTags.write(allocator, tagsWriter, index, unit.getTags());
            ListVector cells = (ListVector) root.getVector(H3_3);
            short[] unitCells = unit.getCells();
            int cellStart = cells.startNewValue(index);
            for (int i = 0; i < unitCells.length; i++) {
                ((SmallIntVector) cells.getDataVector()).setSafe(cellStart + i, unitCells[i]);
            }
            cells.endValue(index, unitCells.length);
            Geometry geometry = directory.geometry(unit);
            if (geometry == null) {
                return 0;
            }
            byte[] wkb = wkbWriter.write(geometry);
            ((VarBinaryVector) root.getVector(GEOM)).setSafe(index, wkb);
            ((Float8Vector) root.getVector(AREA_M2)).setSafe(index, unit.getAreaM2());
            ((Float8Vector) root.getVector(OWN_AREA_M2)).setSafe(index, unit.getOwnAreaM2());
            return wkb.length;
        }

        @Override
        void finish(VectorSchemaRoot root, int count) {
            namesWriter.setValueCount(count);
            tagsWriter.setValueCount(count);
        }
    }

    private static final class RegionsReader extends BatchReader {
        private final AdminDirectory directory;
        private final int[] partNumbers;

        private RegionsReader(BufferAllocator allocator, AdminDirectory directory) {
            super(allocator, regionsSchema(), directory.regionCount());
            this.directory = directory;
            this.partNumbers = directory.regionPartNumbers();
        }

        @Override
        long fill(VectorSchemaRoot root, int index, int row, WKBWriter wkbWriter) {
            AdminUnit unit = directory.regionUnit(row);
            ((BigIntVector) root.getVector(UNIT_ID)).setSafe(index, unit.getId());
            ((IntVector) root.getVector(PART_NO)).setSafe(index, partNumbers[row]);
            ((SmallIntVector) root.getVector(ADMIN_LEVEL)).setSafe(index, unit.getLevel());
            byte[] wkb = wkbWriter.write(directory.region(row));
            ((VarBinaryVector) root.getVector(GEOM)).setSafe(index, wkb);
            return wkb.length;
        }
    }
}
