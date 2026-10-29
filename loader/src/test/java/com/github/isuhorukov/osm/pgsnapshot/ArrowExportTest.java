package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ArrowExportTest {

    private static final Path OSMIUM = Paths.get("/usr/bin/osmium");
    private static final String EXPECTED = "/pipeline/arrow_export_expected.txt";
    private static final String SECTION = "== ";
    private static final String MULTIPOLYGON_PARQUET = "multipolygon.parquet";
    private static final List<String> ENTITIES = List.of(ResultLayout.NODES_DIR, ResultLayout.WAYS_DIR,
            ResultLayout.RELATIONS_DIR);

    @TempDir
    Path tempDir;

    @BeforeAll
    static void osmiumAvailable() {
        assumeTrue(Files.isExecutable(OSMIUM), OSMIUM + " is required");
    }

    @ParameterizedTest
    @EnumSource(ArrowFormat.class)
    void run_arrowFormat_writesPinnedLayoutSchemaAndRows(ArrowFormat format) throws Exception {
        Path pbf = fixturePbf();

        assertEquals(0, OsmPbfTransformation.run(new String[]{"-source_pbf", pbf.toString(), "-osmium_docker", "false",
                "-arrow_format", format.name(), "-result_in_tsv", "false", "-worker_threads", "2"}));

        Path result = pbf.resolveSibling("arrow_export_loc_ways");
        Path arrow = result.resolve(ResultLayout.ARROW_DIR);
        Map<String, String> expected = expectedSections();
        assertEquals(expected.get(format + " layout"), layout(result));
        if (format == ArrowFormat.PARQUET) {
            assertEquals(expected.get(format + " schema"), parquetSchema(arrow));
            assertEquals(expected.get("rows"), parquetRows(arrow));
        } else {
            assertEquals(expected.get(format + " schema"), ipcSchema(arrow) + "\n" + parquetFileSchema(arrow,
                    arrow.resolve(MULTIPOLYGON_PARQUET)));
            assertEquals(expected.get("rows"), ipcRows(arrow) + "\n" + parquetFileRows("multipolygon",
                    arrow.resolve(MULTIPOLYGON_PARQUET)));
        }
    }

    private Path fixturePbf() throws IOException, InterruptedException {
        Path osm = tempDir.resolve("arrow_export.osm");
        try (InputStream fixture = getClass().getResourceAsStream("/pipeline/arrow_export.osm")) {
            Files.copy(Objects.requireNonNull(fixture), osm);
        }
        Path pbf = tempDir.resolve("arrow_export.osm.pbf");
        Process process = new ProcessBuilder(OSMIUM.toString(), "cat", osm.toString(), "-o", pbf.toString())
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertEquals(0, process.waitFor());
        Files.delete(osm);
        return pbf;
    }

    private static Map<String, String> expectedSections() throws IOException {
        Map<String, String> sections = new LinkedHashMap<>();
        try (InputStream expected = ArrowExportTest.class.getResourceAsStream(EXPECTED)) {
            String name = null;
            List<String> lines = new ArrayList<>();
            for (String line : new String(Objects.requireNonNull(expected).readAllBytes(), StandardCharsets.UTF_8)
                    .split("\n")) {
                if (line.startsWith(SECTION)) {
                    putSection(sections, name, lines);
                    name = line.substring(SECTION.length());
                    lines = new ArrayList<>();
                } else {
                    lines.add(line);
                }
            }
            putSection(sections, name, lines);
        }
        return sections;
    }

    private static void putSection(Map<String, String> sections, String name, List<String> lines) {
        if (name != null) {
            sections.put(name, String.join("\n", lines).strip());
        }
    }

    private static String layout(Path result) throws IOException {
        try (Stream<Path> files = Files.walk(result)) {
            return files.filter(Files::isRegularFile).map(file -> result.relativize(file).toString()).sorted()
                    .collect(Collectors.joining("\n"));
        }
    }

    private static List<Path> entityFiles(Path arrow, String entity) throws IOException {
        try (Stream<Path> files = Files.list(arrow.resolve(entity))) {
            return files.sorted().collect(Collectors.toList());
        }
    }

    private static String parquetSchema(Path arrow) throws IOException, SQLException {
        List<String> schema = new ArrayList<>();
        for (String entity : ENTITIES) {
            for (Path file : entityFiles(arrow, entity)) {
                schema.add(parquetFileSchema(arrow, file));
            }
        }
        schema.add(parquetFileSchema(arrow, arrow.resolve(MULTIPOLYGON_PARQUET)));
        return String.join("\n", schema);
    }

    private static String parquetFileSchema(Path arrow, Path file) throws SQLException {
        List<String> lines = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT num_rows, num_row_groups FROM parquet_file_metadata('"
                    + file + "')")) {
                rs.next();
                lines.add(arrow.relativize(file) + " rows=" + rs.getLong(1) + " row_groups=" + rs.getLong(2)
                        + " compression=" + compression(statement, file));
            }
            try (ResultSet rs = statement.executeQuery("SELECT name, type, repetition_type, converted_type, "
                    + "num_children FROM parquet_schema('" + file + "')")) {
                while (rs.next()) {
                    lines.add("  " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3) + " "
                            + rs.getString(4) + " " + rs.getString(5));
                }
            }
            try (ResultSet rs = statement.executeQuery("SELECT decode(value) FROM parquet_kv_metadata('" + file
                    + "') WHERE decode(key) = 'geo'")) {
                while (rs.next()) {
                    lines.add("  geo " + rs.getString(1));
                }
            }
        }
        return String.join("\n", lines);
    }

    private static String compression(Statement statement, Path file) throws SQLException {
        try (ResultSet rs = statement.executeQuery("SELECT string_agg(DISTINCT compression, ',' ORDER BY compression) "
                + "FROM parquet_metadata('" + file + "')")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String parquetRows(Path arrow) throws IOException, SQLException {
        List<String> rows = new ArrayList<>();
        for (String entity : ENTITIES) {
            for (Path file : entityFiles(arrow, entity)) {
                rows.add(parquetFileRows(entity, file));
            }
        }
        rows.add(parquetFileRows("multipolygon", arrow.resolve(MULTIPOLYGON_PARQUET)));
        return String.join("\n", rows);
    }

    private static String parquetFileRows(String entity, Path file) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            List<String[]> columns = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery("DESCRIBE SELECT * FROM read_parquet('" + file + "')")) {
                while (rs.next()) {
                    columns.add(new String[]{rs.getString("column_name"), rs.getString("column_type")});
                }
            }
            List<String> select = new ArrayList<>();
            for (String[] column : columns) {
                String name = '"' + column[0] + '"';
                if (isMap(column)) {
                    select.add("map_keys(" + name + "), map_values(" + name + ")");
                } else {
                    select.add(isGeometry(column) ? "ST_AsWKB(" + name + ")" : name);
                }
            }
            try (ResultSet rs = statement.executeQuery("SELECT " + String.join(", ", select) + " FROM read_parquet('"
                    + file + "', file_row_number = true) ORDER BY file_row_number")) {
                while (rs.next()) {
                    rows.add(entity + " " + parquetRow(rs, columns));
                }
            }
        }
        return String.join("\n", rows);
    }

    private static boolean isMap(String[] column) {
        return column[1].startsWith("MAP(");
    }

    private static boolean isGeometry(String[] column) {
        return column[1].startsWith("GEOMETRY");
    }

    private static String parquetRow(ResultSet rs, List<String[]> columns) throws SQLException {
        List<String> values = new ArrayList<>();
        int index = 1;
        for (String[] column : columns) {
            Object value;
            if (isMap(column)) {
                value = zip(listValue(rs.getObject(index)), listValue(rs.getObject(index + 1)));
                index += 2;
            } else if ("BLOB".equals(column[1]) || isGeometry(column)) {
                value = rs.getBytes(index++);
            } else {
                value = listValue(rs.getObject(index++));
            }
            values.add(column[0] + "=" + text(value));
        }
        return String.join(" ", values);
    }

    private static Object listValue(Object value) throws SQLException {
        return value instanceof Array ? Arrays.asList((Object[]) ((Array) value).getArray()) : value;
    }

    private static Object zip(Object keys, Object values) {
        if (keys == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        List<?> keyList = (List<?>) keys;
        List<?> valueList = (List<?>) values;
        for (int i = 0; i < keyList.size(); i++) {
            map.put(String.valueOf(keyList.get(i)), valueList.get(i));
        }
        return map;
    }

    private static String ipcSchema(Path arrow) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferAllocator allocator = new RootAllocator()) {
            for (String entity : ENTITIES) {
                for (Path file : entityFiles(arrow, entity)) {
                    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
                         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        long rows = 0;
                        while (reader.loadNextBatch()) {
                            rows += root.getRowCount();
                        }
                        lines.add(arrow.relativize(file) + " rows=" + rows + " record_batches="
                                + reader.getRecordBlocks().size());
                        for (Field field : root.getSchema().getFields()) {
                            lines.add("  " + field);
                        }
                    }
                }
            }
        }
        return String.join("\n", lines);
    }

    private static String ipcRows(Path arrow) throws IOException {
        List<String> rows = new ArrayList<>();
        try (BufferAllocator allocator = new RootAllocator()) {
            for (String entity : ENTITIES) {
                for (Path file : entityFiles(arrow, entity)) {
                    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
                         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        while (reader.loadNextBatch()) {
                            for (int row = 0; row < root.getRowCount(); row++) {
                                rows.add(entity + " " + ipcRow(root, row));
                            }
                        }
                    }
                }
            }
        }
        return String.join("\n", rows);
    }

    private static String ipcRow(VectorSchemaRoot root, int row) {
        List<String> values = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            Object value = vector.getObject(row);
            if (vector instanceof MapVector) {
                value = ipcMap((List<?>) value);
            }
            values.add(vector.getName() + "=" + text(value));
        }
        return String.join(" ", values);
    }

    private static Object ipcMap(List<?> entries) {
        if (entries == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (Object entry : entries) {
            Map<?, ?> pair = (Map<?, ?>) entry;
            map.put(String.valueOf(pair.get(MapVector.KEY_NAME)), pair.get(MapVector.VALUE_NAME));
        }
        return map;
    }

    private static String text(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[]) {
            StringBuilder hex = new StringBuilder();
            for (byte b : (byte[]) value) {
                hex.append(String.format("%02X", b));
            }
            return hex.toString();
        }
        if (value instanceof String || value instanceof Text) {
            return quote(value.toString());
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).entrySet().stream()
                    .map(entry -> quote(String.valueOf(entry.getKey())) + ": " + text(entry.getValue()))
                    .collect(Collectors.joining(", ", "{", "}"));
        }
        if (value instanceof List) {
            return ((List<?>) value).stream().map(ArrowExportTest::text).collect(Collectors.joining(", ", "[", "]"));
        }
        return value.toString();
    }

    private static String quote(String value) {
        StringBuilder quoted = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '"' || c == '\\') {
                quoted.append('\\').append(c);
            } else if (c < 0x20) {
                quoted.append(String.format("\\u%04x", (int) c));
            } else {
                quoted.append(c);
            }
        }
        return quoted.append('"').toString();
    }
}
