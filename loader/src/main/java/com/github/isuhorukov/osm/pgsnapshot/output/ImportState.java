package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.pbfextractor.ReplicationHeader;
import com.github.isuhorukov.osm.pgsnapshot.util.HStoreFormatSerializer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public final class ImportState {

    public static final String OSM_IMPORT_STATE_TSV = "osm_import_state.tsv";
    public static final String METADATA_KEY = "openstreetmap_h3:import_state";
    public static final String LOAD = "load";
    public static final String UPDATE = "update";

    private ImportState() {
    }

    public static String parametersJson(CliParameters parameters, List<TagExtension> tagExtensions) {
        List<TagExtension> sortedExtensions = new ArrayList<>(tagExtensions);
        sortedExtensions.sort(Comparator.comparing(TagExtension::getName));
        StringBuilder json = new StringBuilder("{\"extensions\":[");
        for (int i = 0; i < sortedExtensions.size(); i++) {
            TagExtension extension = sortedExtensions.get(i);
            json.append(i == 0 ? "" : ",").append("{\"fingerprint\":");
            jsonString(json, extension.getFingerprint());
            json.append(",\"multipolygon_tag_keys\":");
            jsonStrings(json, new TreeSet<>(extension.multipolygonTagKeys()));
            json.append(",\"name\":");
            jsonString(json, extension.getName());
            json.append('}');
        }
        return json.append("],\"preserve_all_nodes\":").append(parameters.isPreserveAllNodes())
                .append(",\"scale_approx_calc\":").append(parameters.isScaleApproximation())
                .append(",\"skip_buildings\":").append(parameters.isSkipBuildings())
                .append(",\"skip_highway\":").append(parameters.isSkipHighway())
                .append('}').toString();
    }

    public static byte[] tsv(String operation, File sourcePbf, ReplicationHeader replication, String parametersJson) {
        StringBuilder row = new StringBuilder(operation).append('\t');
        HStoreFormatSerializer.escapeString(row, sourcePbf.getName());
        row.append('\t').append(replication.getTimestamp() != null ? replication.getTimestamp().toString()
                : HStoreFormatSerializer.NULL_STRING);
        row.append('\t').append(replication.getSequence() != null ? replication.getSequence().toString()
                : HStoreFormatSerializer.NULL_STRING);
        row.append('\t');
        HStoreFormatSerializer.escapeString(row, replication.getBaseUrl());
        row.append('\t');
        HStoreFormatSerializer.escapeString(row, parametersJson);
        return row.append('\n').toString().getBytes(StandardCharsets.UTF_8);
    }

    public static String json(String operation, File sourcePbf, ReplicationHeader replication, String parametersJson) {
        StringBuilder json = new StringBuilder("{\"operation\":");
        jsonString(json, operation);
        json.append(",\"source_pbf\":");
        jsonString(json, sourcePbf.getName());
        json.append(",\"replication_timestamp\":");
        if (replication.getTimestamp() != null) {
            jsonString(json, replication.getTimestamp().toString());
        } else {
            json.append("null");
        }
        json.append(",\"replication_sequence\":").append(replication.getSequence())
                .append(",\"replication_url\":");
        if (replication.getBaseUrl() != null) {
            jsonString(json, replication.getBaseUrl());
        } else {
            json.append("null");
        }
        return json.append(",\"parameters\":").append(parametersJson).append('}').toString();
    }

    public static void write(File resultDirectory, byte[] tsv) throws IOException {
        File metadataDirectory = new File(resultDirectory, ResultLayout.IMPORT_RELATED_METADATA_DIR);
        try (OutputStream output = new FileOutputStream(new File(metadataDirectory, OSM_IMPORT_STATE_TSV))) {
            output.write(tsv);
        }
    }

    private static void jsonStrings(StringBuilder json, Collection<String> values) {
        json.append('[');
        boolean first = true;
        for (String value : values) {
            json.append(first ? "" : ",");
            jsonString(json, value);
            first = false;
        }
        json.append(']');
    }

    static void jsonString(StringBuilder json, String value) {
        json.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') {
                json.append('\\').append(c);
            } else if (c < 0x20) {
                json.append(String.format("\\u%04x", (int) c));
            } else {
                json.append(c);
            }
        }
        json.append('"');
    }
}
