package com.github.isuhorukov.osm.pgsnapshot.output;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ResourceCopier {

    public static final String DATABASE_INIT_SQL = "database_init.sql";
    public static final String DATABASE_AFTER_INIT_SQL = "database_after_init.sql";
    public static final String MULTIPOLYGON_SQL = "multipolygon.sql";
    public static final String MULTIPOLYGON_IMPORT_SQL = "multipolygon_import.sql";
    public static final String RELATIONS_IMPORT_SQL = "relations_import.sql";
    public static final String WAYS_32767_IMPORT_SQL = "ways_32767_import.sql";
    public static final String WAYS_DEFAULT_PARTITION_SQL = "ways_default_partition.sql";
    public static final String NODES_ANALYZE_SQL = "nodes_analyze.sql";
    public static final String H3_POLY_TSV_GZ = "h3_poly.tsv.gz";
    public static final String OSMIUM_EXPORT_JSON = "osmium_export.json";
    private static final String SUBSTITUTE = "${substitute}";

    private ResourceCopier() {
    }

    public static void copyOsmiumSettings(File resultDir) throws IOException {
        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        copyResource("/" + OSMIUM_EXPORT_JSON, new File(staticDir, OSMIUM_EXPORT_JSON));
    }

    public static void copyResources(File resultDir, boolean columnarStorage) throws IOException {
        copyResources(resultDir, columnarStorage, Collections.emptySet());
    }

    public static void copyResources(File resultDir, boolean columnarStorage, Set<String> multipolygonTagKeys) throws IOException {
        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        copyResource("/" + H3_POLY_TSV_GZ, new File(staticDir, H3_POLY_TSV_GZ));
        write(new File(staticDir, DATABASE_INIT_SQL), databaseInitScript(columnarStorage));
        write(new File(staticDir, MULTIPOLYGON_SQL), multipolygonScript(columnarStorage, multipolygonTagKeys));
        copyResource("/" + DATABASE_AFTER_INIT_SQL, new File(staticDir, DATABASE_AFTER_INIT_SQL));
    }

    public static void copyImportScripts(File resultDir, boolean columnarStorage) throws IOException {
        File sqlDir = new File(resultDir, ResultLayout.SQL_DIR);
        File sqlFinalizeDir = new File(resultDir, ResultLayout.SQL_FINALIZE_DIR);
        copyResource("/" + RELATIONS_IMPORT_SQL, new File(sqlDir, RELATIONS_IMPORT_SQL));
        write(new File(sqlDir, WAYS_32767_IMPORT_SQL), ways32767ImportScript(columnarStorage));
        copyResource("/" + WAYS_DEFAULT_PARTITION_SQL, new File(sqlFinalizeDir, WAYS_DEFAULT_PARTITION_SQL));
        copyResource("/" + NODES_ANALYZE_SQL, new File(sqlFinalizeDir, NODES_ANALYZE_SQL));
        copyResource("/" + MULTIPOLYGON_IMPORT_SQL, new File(sqlFinalizeDir, MULTIPOLYGON_IMPORT_SQL));
    }

    public static String databaseInitScript(boolean columnarStorage) throws IOException {
        return resourceWithSubstitutes("/" + DATABASE_INIT_SQL, Collections.singletonMap(SUBSTITUTE, columnarStorage
                ? "CREATE EXTENSION citus;\nALTER SYSTEM SET columnar.compression TO 'none';\nSELECT pg_reload_conf();" : ""));
    }

    public static String multipolygonScript(boolean columnarStorage, Set<String> multipolygonTagKeys) throws IOException {
        Map<String, String> multipolygonSubstitutes = new LinkedHashMap<>();
        multipolygonSubstitutes.put(SUBSTITUTE, columnarStorage ? "USING COLUMNAR" : "");
        multipolygonSubstitutes.put("${multipolygon_tags}", multipolygonTagKeys.isEmpty()
                ? "tags" : "coalesce(p.tags || r.extension_tags, p.tags) tags");
        multipolygonSubstitutes.put("${multipolygon_tags_join}", multipolygonTagKeys.isEmpty()
                ? "" : " left join (select id, slice(tags, " + sqlTextArray(multipolygonTagKeys) + ") extension_tags"
                + " from relations where tags ?| " + sqlTextArray(multipolygonTagKeys) + ") r on r.id=p.id");
        return resourceWithSubstitutes("/" + MULTIPOLYGON_SQL, multipolygonSubstitutes);
    }

    public static String ways32767ImportScript(boolean columnarStorage) throws IOException {
        return resourceWithSubstitutes("/" + WAYS_32767_IMPORT_SQL, Collections.singletonMap(SUBSTITUTE,
                columnarStorage ? "SELECT alter_table_set_access_method('ways_32767', 'columnar');" : ""));
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

    private static String sqlTextArray(Collection<String> values) {
        return values.stream().map(value -> "'" + value.replace("'", "''") + "'")
                .collect(Collectors.joining(",", "ARRAY[", "]"));
    }
}
