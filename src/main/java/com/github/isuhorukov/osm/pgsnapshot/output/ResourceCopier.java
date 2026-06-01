package com.github.isuhorukov.osm.pgsnapshot.output;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ResourceCopier {

    private ResourceCopier() {
    }

    public static void copyOsmiumSettings(File resultDir) throws IOException {
        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        copyResource("/osmium_export.json", new File(staticDir, "osmium_export.json"));
    }

    public static void copyResources(File resultDir, boolean columnarStorage) throws IOException {
        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        copyResource("/h3_poly.tsv.gz", new File(staticDir, "h3_poly.tsv.gz"));
        copyResourceWithSubstitute("/database_init.sql",
                new File(staticDir, "database_init.sql"),
                columnarStorage ? "CREATE EXTENSION citus;\nALTER SYSTEM SET columnar.compression TO 'none';\nSELECT pg_reload_conf();" : "");
        copyResourceWithSubstitute("/multipolygon.sql", new File(staticDir, "multipolygon.sql"),
                columnarStorage ? "USING COLUMNAR" : "");
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
        try (OutputStream outputStream = new FileOutputStream(destination);
             InputStream resourceStream = ResourceCopier.class.getResourceAsStream(classpath)) {
            if (resourceStream == null) {
                throw new IllegalArgumentException("Resource not found: " + classpath);
            }
            String resourceContent = IOUtils.toString(resourceStream, StandardCharsets.UTF_8).replace("${substitute}", substituteValue);
            IOUtils.write(resourceContent, outputStream, StandardCharsets.UTF_8);
        }
    }
}
