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

    private ResourceCopier() {
    }

    public static void copyOsmiumSettings(File resultDir) throws IOException {
        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        copyResource("/osmium_export.json", new File(staticDir, "osmium_export.json"));
    }

    public static void copyResources(File resultDir, boolean columnarStorage) throws IOException {
        copyResources(resultDir, columnarStorage, Collections.emptySet());
    }

    public static void copyResources(File resultDir, boolean columnarStorage, Set<String> multipolygonTagKeys) throws IOException {
        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        copyResource("/h3_poly.tsv.gz", new File(staticDir, "h3_poly.tsv.gz"));
        copyResourceWithSubstitute("/database_init.sql",
                new File(staticDir, "database_init.sql"),
                columnarStorage ? "CREATE EXTENSION citus;\nALTER SYSTEM SET columnar.compression TO 'none';\nSELECT pg_reload_conf();" : "");
        Map<String, String> multipolygonSubstitutes = new LinkedHashMap<>();
        multipolygonSubstitutes.put("${substitute}", columnarStorage ? "USING COLUMNAR" : "");
        multipolygonSubstitutes.put("${multipolygon_tags}", multipolygonTagKeys.isEmpty()
                ? "tags" : "coalesce(p.tags || r.extension_tags, p.tags) tags");
        multipolygonSubstitutes.put("${multipolygon_tags_join}", multipolygonTagKeys.isEmpty()
                ? "" : " left join (select id, slice(tags, " + sqlTextArray(multipolygonTagKeys) + ") extension_tags"
                + " from relations where tags ?| " + sqlTextArray(multipolygonTagKeys) + ") r on r.id=p.id");
        copyResourceWithSubstitutes("/multipolygon.sql", new File(staticDir, "multipolygon.sql"), multipolygonSubstitutes);
        copyResourceWithSubstitute("/database_after_init.sql",
                new File(staticDir, "database_after_init.sql"),
                columnarStorage ? "SELECT alter_table_set_access_method('ways_32767', 'columnar');" : "");
    }

    public static void copyResource(String classpath, File destination) throws IOException {
        try (OutputStream outputStream = new FileOutputStream(destination);
             InputStream resourceStream = ResourceCopier.class.getResourceAsStream(classpath)) {
            if (resourceStream == null) {
                throw new IllegalArgumentException("Resource not found: " + classpath);
            }
            IOUtils.copy(resourceStream, outputStream);
        }
    }

    public static void copyResourceWithSubstitute(String classpath, File destination, String substituteValue) throws IOException {
        copyResourceWithSubstitutes(classpath, destination, Collections.singletonMap("${substitute}", substituteValue));
    }

    public static void copyResourceWithSubstitutes(String classpath, File destination, Map<String, String> substitutes) throws IOException {
        try (OutputStream outputStream = new FileOutputStream(destination);
             InputStream resourceStream = ResourceCopier.class.getResourceAsStream(classpath)) {
            if (resourceStream == null) {
                throw new IllegalArgumentException("Resource not found: " + classpath);
            }
            String resourceContent = IOUtils.toString(resourceStream, StandardCharsets.UTF_8);
            for (Map.Entry<String, String> substitute : substitutes.entrySet()) {
                resourceContent = resourceContent.replace(substitute.getKey(), substitute.getValue());
            }
            IOUtils.write(resourceContent, outputStream, StandardCharsets.UTF_8);
        }
    }

    private static String sqlTextArray(Collection<String> values) {
        return values.stream().map(value -> "'" + value.replace("'", "''") + "'")
                .collect(Collectors.joining(",", "ARRAY[", "]"));
    }
}
