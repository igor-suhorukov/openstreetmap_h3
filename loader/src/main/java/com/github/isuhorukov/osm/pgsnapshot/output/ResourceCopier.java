package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.util.PartitionSplitter;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ResourceCopier {

    public static final String DATABASE_INIT_SQL = "database_init.sql";
    public static final String DATABASE_AFTER_INIT_SQL = "database_after_init.sql";
    public static final String MULTIPOLYGON_SQL = "multipolygon.sql";
    public static final String MULTIPOLYGON_INSERT_SQL = "multipolygon_insert.sql";
    public static final String MULTIPOLYGON_IMPORT_SQL = "multipolygon_import.sql";
    public static final String RELATIONS_IMPORT_SQL = "relations_import.sql";
    public static final String WAYS_32767_IMPORT_SQL = "ways_32767_import.sql";
    public static final String WAYS_DEFAULT_PARTITION_SQL = "ways_default_partition.sql";
    public static final String NODES_ANALYZE_SQL = "nodes_analyze.sql";
    public static final String H3_POLY_TSV_GZ = "h3_poly.tsv.gz";
    public static final String OSMIUM_EXPORT_JSON = "osmium_export.json";
    public static final String UPDATE_PREPARE_SQL = "update_prepare.sql";
    public static final String UPDATE_DELTA_SQL = "update_delta.sql";
    public static final String UPDATE_APPLY_SQL = "update_apply.sql";
    public static final String UPDATE_MULTIPOLYGON_SQL = "update_multipolygon.sql";
    public static final String UPDATE_AFFECTED_RELATIONS_SQL = "update_affected_relations.sql";
    public static final String UPDATE_AREA_RELATIONS_SQL = "update_area_relations.sql";
    private static final String UPDATABLE_SCHEMA_SQL = "updatable_schema.sql";
    private static final String OSM_IMPORT_STATE_SQL = "osm_import_state.sql";
    private static final String SUBSTITUTE = "${substitute}";
    private static final String OSM_IMPORT_STATE = "${osm_import_state}";

    private ResourceCopier() {
    }

    public static void copyOsmiumSettings(File resultDir) throws IOException {
        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        copyResource("/" + OSMIUM_EXPORT_JSON, new File(staticDir, OSMIUM_EXPORT_JSON));
    }

    public static void copyResources(File resultDir, boolean columnarStorage) throws IOException {
        copyResources(resultDir, columnarStorage, false);
    }

    public static void copyResources(File resultDir, boolean columnarStorage, boolean updatable) throws IOException {
        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        copyResource("/" + H3_POLY_TSV_GZ, new File(staticDir, H3_POLY_TSV_GZ));
        write(new File(staticDir, DATABASE_INIT_SQL), databaseInitScript(columnarStorage, updatable));
        write(new File(staticDir, MULTIPOLYGON_SQL), multipolygonScript(columnarStorage));
        write(new File(staticDir, MULTIPOLYGON_INSERT_SQL), multipolygonInsertScript());
        write(new File(staticDir, DATABASE_AFTER_INIT_SQL), databaseAfterInitScript(updatable));
    }

    public static void copyImportScripts(File resultDir, boolean columnarStorage) throws IOException {
        copyImportScripts(resultDir, columnarStorage, false);
    }

    public static void copyImportScripts(File resultDir, boolean columnarStorage, boolean updatable) throws IOException {
        File sqlDir = new File(resultDir, ResultLayout.SQL_DIR);
        File sqlFinalizeDir = new File(resultDir, ResultLayout.SQL_FINALIZE_DIR);
        write(new File(sqlDir, RELATIONS_IMPORT_SQL), relationsImportScript(updatable));
        write(new File(sqlDir, WAYS_32767_IMPORT_SQL), ways32767ImportScript(columnarStorage, updatable));
        write(new File(sqlFinalizeDir, WAYS_DEFAULT_PARTITION_SQL), waysDefaultPartitionScript(updatable));
        write(new File(sqlFinalizeDir, NODES_ANALYZE_SQL), nodesAnalyzeScript(updatable));
        copyResource("/" + MULTIPOLYGON_IMPORT_SQL, new File(sqlFinalizeDir, MULTIPOLYGON_IMPORT_SQL));
    }

    public static String databaseInitScript(boolean columnarStorage) throws IOException {
        return databaseInitScript(columnarStorage, false);
    }

    public static String databaseInitScript(boolean columnarStorage, boolean updatable) throws IOException {
        Map<String, String> substitutes = new LinkedHashMap<>();
        substitutes.put(SUBSTITUTE, columnarStorage
                ? "CREATE EXTENSION citus;\nALTER SYSTEM SET columnar.compression TO 'none';\nSELECT pg_reload_conf();" : "");
        substitutes.put("${updatable_schema}", updatable ? "\n" + resource("/" + UPDATABLE_SCHEMA_SQL).trim() : "");
        return resourceWithSubstitutes("/" + DATABASE_INIT_SQL, substitutes);
    }

    public static String databaseAfterInitScript(boolean updatable) throws IOException {
        return resourceWithSubstitutes("/" + DATABASE_AFTER_INIT_SQL,
                Collections.singletonMap(OSM_IMPORT_STATE, updatable ? osmImportStateCopy() : ""));
    }

    public static String multipolygonScript(boolean columnarStorage) throws IOException {
        return resourceWithSubstitutes("/" + MULTIPOLYGON_SQL,
                Collections.singletonMap(SUBSTITUTE, columnarStorage ? "USING COLUMNAR" : ""));
    }

    public static String multipolygonInsertScript() throws IOException {
        return resource("/" + MULTIPOLYGON_INSERT_SQL);
    }

    public static String relationsImportScript(boolean updatable) throws IOException {
        Map<String, String> substitutes = new LinkedHashMap<>();
        substitutes.put("${relation_version_column}", updatable ? PartitionSplitter.VERSION_COLUMN : "");
        substitutes.put("${relations_primary_key}", updatable
                ? "\nALTER TABLE ONLY relations ADD CONSTRAINT pk_relations PRIMARY KEY (id) INCLUDE (version);" : "");
        return resourceWithSubstitutes("/" + RELATIONS_IMPORT_SQL, substitutes);
    }

    public static String ways32767ImportScript(boolean columnarStorage) throws IOException {
        return ways32767ImportScript(columnarStorage, false);
    }

    public static String ways32767ImportScript(boolean columnarStorage, boolean updatable) throws IOException {
        Map<String, String> substitutes = new LinkedHashMap<>();
        substitutes.put(SUBSTITUTE, columnarStorage ? "SELECT alter_table_set_access_method('ways_32767', 'columnar');" : "");
        substitutes.put("${way_columns}", PartitionSplitter.wayColumns(updatable));
        substitutes.put("${way_version_columns}", updatable ? PartitionSplitter.WAY_VERSION_COLUMNS : "");
        substitutes.put("${way_version_not_null}", updatable
                ? "\nalter table ways_32767 alter COLUMN version set NOT NULL;"
                + "\nalter table ways_32767 alter COLUMN geom_hash set NOT NULL;" : "");
        return resourceWithSubstitutes("/" + WAYS_32767_IMPORT_SQL, substitutes);
    }

    public static String waysDefaultPartitionScript(boolean updatable) throws IOException {
        return resourceWithSubstitutes("/" + WAYS_DEFAULT_PARTITION_SQL, Collections.singletonMap("${ways_id_index}",
                "\nCREATE UNIQUE INDEX idx_ways_id ON ways (id, h3_3)"
                        + (updatable ? " INCLUDE (version, geom_hash)" : "") + ";"));
    }

    public static String nodesAnalyzeScript(boolean updatable) throws IOException {
        return resourceWithSubstitutes("/" + NODES_ANALYZE_SQL, Collections.singletonMap("${nodes_id_index}",
                "\nCREATE UNIQUE INDEX idx_nodes_id ON nodes (id, h3_3)" + (updatable ? " INCLUDE (version)" : "") + ";"));
    }

    public static String updateScript(String name) throws IOException {
        Map<String, String> substitutes = new LinkedHashMap<>();
        substitutes.put("${node_columns}", PartitionSplitter.nodeColumns(true));
        substitutes.put("${way_columns}", PartitionSplitter.wayColumns(true));
        substitutes.put("${relation_columns}", PartitionSplitter.relationColumns(true));
        substitutes.put(OSM_IMPORT_STATE, osmImportStateCopy());
        return resourceWithSubstitutes("/" + name, substitutes);
    }

    private static String osmImportStateCopy() throws IOException {
        return "\n" + resource("/" + OSM_IMPORT_STATE_SQL).trim();
    }

    public static String resource(String classpath) throws IOException {
        return resourceWithSubstitutes(classpath, Collections.emptyMap());
    }

    public static byte[] resourceBytes(String classpath) throws IOException {
        try (InputStream resourceStream = openResource(classpath)) {
            return IOUtils.toByteArray(resourceStream);
        }
    }

    public static InputStream openResource(String classpath) {
        InputStream resourceStream = ResourceCopier.class.getResourceAsStream(classpath);
        if (resourceStream == null) {
            throw new IllegalArgumentException("Resource not found: " + classpath);
        }
        return resourceStream;
    }

    public static void copyResource(String classpath, File destination) throws IOException {
        try (InputStream resourceStream = openResource(classpath);
             OutputStream outputStream = new FileOutputStream(destination)) {
            IOUtils.copy(resourceStream, outputStream);
        }
    }

    public static String resourceWithSubstitutes(String classpath, Map<String, String> substitutes) throws IOException {
        String resourceContent;
        try (InputStream resourceStream = openResource(classpath)) {
            resourceContent = IOUtils.toString(resourceStream, StandardCharsets.UTF_8);
        }
        for (Map.Entry<String, String> substitute : substitutes.entrySet()) {
            resourceContent = resourceContent.replace(substitute.getKey(), substitute.getValue());
        }
        return resourceContent;
    }

    private static void write(File destination, String content) throws IOException {
        try (OutputStream outputStream = new FileOutputStream(destination)) {
            IOUtils.write(content, outputStream, StandardCharsets.UTF_8);
        }
    }
}
