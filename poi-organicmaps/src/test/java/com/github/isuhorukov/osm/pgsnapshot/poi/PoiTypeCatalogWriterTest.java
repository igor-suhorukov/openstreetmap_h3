package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PoiTypeCatalogWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void write_catalogTsvAndImportScript() throws Exception {
        Path staticDirectory = Files.createDirectory(tempDir.resolve("static"));
        Path sqlDirectory = Files.createDirectory(tempDir.resolve("sql"));
        OrganicMapsPoiClassifier classifier = OrganicMapsPoiClassifier.load(OrganicMapsPoiClassifierTest.testDataDirectory());

        PoiTypeCatalogWriter.write(staticDirectory.toFile(), sqlDirectory.toFile(), classifier.getPoiTypes());

        List<String> rows = Files.readAllLines(staticDirectory.resolve(PoiTypeCatalogWriter.CATALOG_FILE), StandardCharsets.UTF_8);
        assertEquals(classifier.getPoiTypes().size(), rows.size());
        assertTrue(rows.contains("\"highway-bus_stop\"\t\"{category_transport}\"\t\"Bus Stop\"\t\"Остановка\"\t\"ป้ายรถเมล์\""), rows.toString());
        assertTrue(rows.contains("\"amenity-ice_cream\"\t\\N\t\\N\t\\N\t\\N"), rows.toString());

        String script = Files.readString(sqlDirectory.resolve(PoiTypeCatalogWriter.CATALOG_SCRIPT));
        assertTrue(script.contains("/input/static/" + PoiTypeCatalogWriter.CATALOG_FILE));
        for (String language : OrganicMapsPoiClassifier.NAME_LANGUAGES) {
            assertTrue(script.contains("name_" + language + " text"), language);
        }
    }
}
