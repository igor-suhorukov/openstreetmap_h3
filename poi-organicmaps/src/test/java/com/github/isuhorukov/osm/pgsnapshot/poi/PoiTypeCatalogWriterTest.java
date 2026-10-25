package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PoiTypeCatalogWriterTest {

    @Test
    void write_catalogTsvAndImportScript() throws Exception {
        RecordingPostgresqlResources resources = new RecordingPostgresqlResources();
        OrganicMapsPoiClassifier classifier = OrganicMapsPoiClassifier.load(OrganicMapsPoiClassifierTest.testDataDirectory());

        PoiTypeCatalogWriter.write(resources, classifier.getPoiTypes());

        List<String> rows = Arrays.asList(new String(resources.staticFiles.get(PoiTypeCatalogWriter.CATALOG_FILE),
                StandardCharsets.UTF_8).split("\n"));
        assertEquals(classifier.getPoiTypes().size(), rows.size());
        assertTrue(rows.contains("\"highway-bus_stop\"\t\"{category_transport}\"\t\"Bus Stop\"\t\"Остановка\"\t\"ป้ายรถเมล์\""), rows.toString());
        assertTrue(rows.contains("\"amenity-ice_cream\"\t\\N\t\\N\t\\N\t\\N"), rows.toString());

        String script = resources.importScripts.get(PoiTypeCatalogWriter.CATALOG_SCRIPT);
        assertTrue(script.contains("/input/static/" + PoiTypeCatalogWriter.CATALOG_FILE));
        for (String language : OrganicMapsPoiClassifier.NAME_LANGUAGES) {
            assertTrue(script.contains("name_" + language + " text"), language);
        }
        assertEquals(List.of(PoiTypeCatalogWriter.CATALOG_FILE), List.copyOf(resources.staticFiles.keySet()));
        assertEquals(List.of(PoiTypeCatalogWriter.CATALOG_SCRIPT), List.copyOf(resources.importScripts.keySet()));
    }
}
