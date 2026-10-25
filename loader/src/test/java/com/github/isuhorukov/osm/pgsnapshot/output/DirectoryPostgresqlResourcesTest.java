package com.github.isuhorukov.osm.pgsnapshot.output;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DirectoryPostgresqlResourcesTest {

    @TempDir
    Path tempDir;

    @Test
    void add_staticFileAndImportScript_writtenToResultDirectories() throws Exception {
        File resultDirectory = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDirectory, true, false);
        DirectoryPostgresqlResources resources = new DirectoryPostgresqlResources(resultDirectory);

        resources.addStaticFile("om_poi_type.tsv", "\"ป้ายรถเมล์\"\n".getBytes(StandardCharsets.UTF_8));
        resources.addImportScript("om_poi_type.sql", "CREATE TABLE om_poi_type();");

        assertEquals("\"ป้ายรถเมล์\"\n", Files.readString(tempDir.resolve("static/om_poi_type.tsv")));
        assertEquals("CREATE TABLE om_poi_type();", Files.readString(tempDir.resolve("sql/om_poi_type.sql")));
    }

    @Test
    void checkedFileName_pathsRejected() {
        assertEquals("a.sql", DirectoryPostgresqlResources.checkedFileName("a.sql"));
        for (String name : new String[]{null, "", ".", "..", "../a.sql", "sql/a.sql", "/tmp/a.sql"}) {
            assertThrows(IllegalArgumentException.class, () -> DirectoryPostgresqlResources.checkedFileName(name), name);
        }
    }
}
