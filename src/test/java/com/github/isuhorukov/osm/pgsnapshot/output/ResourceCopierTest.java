package com.github.isuhorukov.osm.pgsnapshot.output;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ResourceCopierTest {

    @TempDir
    Path tempDir;

    private File prepareStaticDir() {
        File staticDir = new File(tempDir.toFile(), ResultLayout.STATIC_DIR);
        staticDir.mkdirs();
        return tempDir.toFile();
    }

    @Test
    void copyResource_nonExistentClasspathResource_throwsIllegalArgument() {
        File dest = new File(tempDir.toFile(), "out.txt");
        assertThrows(IllegalArgumentException.class,
                () -> ResourceCopier.copyResource("/no_such_resource_xyz.json", dest));
    }

    @Test
    void copyOsmiumSettings_copiesOsmiumExportJson() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyOsmiumSettings(resultDir);

        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        File copied = new File(staticDir, "osmium_export.json");
        assertTrue(copied.exists());
        assertTrue(copied.length() > 0);
    }

    @Test
    void copyResources_nonColumnar_substituteIsEmpty() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false);

        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        String dbInit = Files.readString(new File(staticDir, "database_init.sql").toPath());
        assertFalse(dbInit.contains("CREATE EXTENSION citus;"),
                "non-columnar init should not contain citus extension");
        assertFalse(dbInit.contains("${substitute}"),
                "placeholder should be replaced");

        String multipolygon = Files.readString(new File(staticDir, "multipolygon.sql").toPath());
        assertFalse(multipolygon.contains("USING COLUMNAR"));
        assertFalse(multipolygon.contains("${substitute}"));

        String afterInit = Files.readString(new File(staticDir, "database_after_init.sql").toPath());
        assertFalse(afterInit.contains("columnar"));
        assertFalse(afterInit.contains("${substitute}"));
    }

    @Test
    void copyResources_columnar_substituteContainsCitusAndColumnar() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, true);

        File staticDir = new File(resultDir, ResultLayout.STATIC_DIR);
        String dbInit = Files.readString(new File(staticDir, "database_init.sql").toPath());
        assertTrue(dbInit.contains("CREATE EXTENSION citus;"));
        assertFalse(dbInit.contains("${substitute}"));

        String multipolygon = Files.readString(new File(staticDir, "multipolygon.sql").toPath());
        assertTrue(multipolygon.contains("USING COLUMNAR"));
        assertFalse(multipolygon.contains("${substitute}"));

        String afterInit = Files.readString(new File(staticDir, "database_after_init.sql").toPath());
        assertTrue(afterInit.contains("SELECT alter_table_set_access_method"));
        assertFalse(afterInit.contains("${substitute}"));
    }

    @Test
    void copyResources_h3PolyGzCopied() throws IOException {
        File resultDir = prepareStaticDir();

        ResourceCopier.copyResources(resultDir, false);

        File h3Poly = new File(new File(resultDir, ResultLayout.STATIC_DIR), "h3_poly.tsv.gz");
        assertTrue(h3Poly.exists());
        assertTrue(h3Poly.length() > 0);
    }
}
