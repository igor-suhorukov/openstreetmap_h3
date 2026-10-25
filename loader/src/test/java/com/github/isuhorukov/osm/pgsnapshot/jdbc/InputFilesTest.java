package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.extension.PostgresqlResources;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class InputFilesTest {

    @Test
    void read_addedContentByInputPath() throws Exception {
        InputFiles inputFiles = new InputFiles();
        inputFiles.add("static/a.sql", "SELECT 1;");
        inputFiles.add("import_related_metadata/b.tsv", new byte[]{'1', '\n'});

        assertEquals("SELECT 1;", inputFiles.read("/input/static/a.sql"));
        try (InputStream inputStream = inputFiles.open("/input/import_related_metadata/b.tsv")) {
            assertEquals("1\n", new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void read_classpathResource() throws Exception {
        InputFiles inputFiles = new InputFiles();
        inputFiles.addResource("static/osmium_export.json", "/osmium_export.json");

        assertTrue(inputFiles.read("/input/static/osmium_export.json").contains("\"area_tags\""));
    }

    @Test
    void open_missingOrOutsideOfInput_throws() {
        InputFiles inputFiles = new InputFiles();
        inputFiles.addResource("static/missing.sql", "/no_such_resource.sql");

        assertThrows(IllegalArgumentException.class, () -> inputFiles.open("/input/static/unknown.sql"));
        assertThrows(IllegalArgumentException.class, () -> inputFiles.open("/tmp/static/a.sql"));
        assertThrows(IllegalArgumentException.class, () -> inputFiles.open("/input/static/missing.sql"));
    }

    @Test
    void jdbcPostgresqlResources_staticFilesAsInputAndScriptsCollected() throws Exception {
        InputFiles inputFiles = new InputFiles();
        JdbcPostgresqlResources resources = new JdbcPostgresqlResources(inputFiles);
        PostgresqlResources extensionResources = resources;

        extensionResources.addStaticFile("om_poi_type.tsv", "\"type\"\n".getBytes(StandardCharsets.UTF_8));
        extensionResources.addImportScript("om_poi_type.sql", "CREATE TABLE om_poi_type();");

        assertEquals("\"type\"\n", inputFiles.read("/input/static/om_poi_type.tsv"));
        assertEquals(java.util.List.of("CREATE TABLE om_poi_type();"), resources.getImportScripts());
        byte[] content = new byte[0];
        assertThrows(IllegalArgumentException.class, () -> extensionResources.addStaticFile("../x.tsv", content));
        assertThrows(IllegalArgumentException.class, () -> extensionResources.addImportScript("sql/x.sql", ""));
    }
}
