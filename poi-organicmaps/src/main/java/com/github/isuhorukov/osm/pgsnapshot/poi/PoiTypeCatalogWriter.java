package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

final class PoiTypeCatalogWriter {

    static final String CATALOG_FILE = "om_poi_type.tsv";
    static final String CATALOG_SCRIPT = "om_poi_type.sql";

    private static final String NULL_VALUE = "\\N";

    private PoiTypeCatalogWriter() {
    }

    static void write(File staticDirectory, File sqlDirectory, List<PoiType> poiTypes) throws IOException {
        StringBuilder tsv = new StringBuilder();
        for (PoiType poiType : poiTypes) {
            appendCsvValue(tsv, poiType.getType());
            tsv.append('\t');
            appendCsvValue(tsv, poiType.getCategories().isEmpty()
                    ? null : OrganicMapsPoiClassifier.toArrayLiteral(poiType.getCategories()));
            for (String language : OrganicMapsPoiClassifier.NAME_LANGUAGES) {
                tsv.append('\t');
                appendCsvValue(tsv, poiType.getName(language));
            }
            tsv.append('\n');
        }
        Files.write(new File(staticDirectory, CATALOG_FILE).toPath(), tsv.toString().getBytes(StandardCharsets.UTF_8));
        try (InputStream script = PoiTypeCatalogWriter.class.getResourceAsStream(CATALOG_SCRIPT)) {
            if (script == null) {
                throw new IllegalStateException("Resource not found: " + CATALOG_SCRIPT);
            }
            Files.copy(script, new File(sqlDirectory, CATALOG_SCRIPT).toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void appendCsvValue(StringBuilder csv, String value) {
        if (value == null) {
            csv.append(NULL_VALUE);
            return;
        }
        csv.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') {
                csv.append('\\');
            }
            csv.append(c);
        }
        csv.append('"');
    }
}
