package com.github.isuhorukov.osm.pgsnapshot.output;

import com.beust.jcommander.JCommander;
import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.pbfextractor.ReplicationHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ImportStateTest {

    @TempDir
    Path tempDir;

    private static TagExtension extension(String name, Set<String> tagKeys, String fingerprint) {
        return new TagExtension() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public List<Tag> additionalTags(Entity entity) {
                return Collections.emptyList();
            }

            @Override
            public Set<String> multipolygonTagKeys() {
                return tagKeys;
            }

            @Override
            public String getFingerprint() {
                return fingerprint;
            }
        };
    }

    private static CliParameters parameters(String... args) {
        CliParameters parameters = new CliParameters();
        JCommander.newBuilder().addObject(parameters).build().parse(args);
        return parameters;
    }

    @Test
    void parametersJson_contentParametersAndSortedExtensions() {
        String json = ImportState.parametersJson(parameters("-source_pbf", "a.osm.pbf", "-skip_buildings",
                        "-worker_threads", "16", "-pg_script_count", "8"),
                List.of(extension("zeta", Set.of("is.z", "is.a"), "f\"1"), extension("alpha", Set.of(), "")));

        assertEquals("{\"extensions\":[{\"fingerprint\":\"\",\"multipolygon_tag_keys\":[],\"name\":\"alpha\"},"
                + "{\"fingerprint\":\"f\\\"1\",\"multipolygon_tag_keys\":[\"is.a\",\"is.z\"],\"name\":\"zeta\"}],"
                + "\"preserve_all_nodes\":false,\"scale_approx_calc\":false,\"skip_buildings\":true,\"skip_highway\":false}",
                json);
    }

    @Test
    void parametersJson_workersAndOutputOptionsIgnored() {
        assertEquals(ImportState.parametersJson(parameters("-source_pbf", "a.osm.pbf"), List.of()),
                ImportState.parametersJson(parameters("-source_pbf", "b.osm.pbf", "-worker_threads", "32",
                        "-pg_jdbc_url", "jdbc:postgresql://localhost/osmworld", "-update"), List.of()));
    }

    @Test
    void jsonString_controlCharactersEscaped() {
        StringBuilder json = new StringBuilder();

        ImportState.jsonString(json, "a\\b\n\u0001");

        assertEquals("\"a\\\\b\\u000a\\u0001\"", json.toString());
    }

    @Test
    void tsv_replicationFieldsAndCsvQuotedJson() {
        ReplicationHeader replication = new ReplicationHeader(Instant.parse("2026-09-04T20:21:02Z"), 4881L,
                "https://download.geofabrik.de/asia/thailand-updates");

        String row = new String(ImportState.tsv(ImportState.UPDATE, new File("/data/thailand-260904.osm.pbf"),
                replication, "{\"a\":\"b\\\\c\"}"), StandardCharsets.UTF_8);

        assertEquals("update\t\"thailand-260904.osm.pbf\"\t2026-09-04T20:21:02Z\t4881\t"
                + "\"https://download.geofabrik.de/asia/thailand-updates\"\t\"{\\\"a\\\":\\\"b\\\\\\\\c\\\"}\"\n", row);
    }

    @Test
    void tsv_withoutReplication_nulls() {
        String row = new String(ImportState.tsv(ImportState.LOAD, new File("sample.osm.pbf"), ReplicationHeader.NONE,
                "{}"), StandardCharsets.UTF_8);

        assertEquals("load\t\"sample.osm.pbf\"\t\\N\t\\N\t\\N\t\"{}\"\n", row);
    }

    @Test
    void write_importRelatedMetadataFile() throws Exception {
        File resultDirectory = tempDir.toFile();
        ResultLayout.prepareResultDirectories(resultDirectory, true, false);

        ImportState.write(resultDirectory, "load\n".getBytes(StandardCharsets.UTF_8));

        assertEquals("load\n", Files.readString(tempDir.resolve("import_related_metadata/osm_import_state.tsv")));
    }

    @Test
    void json_replicationFieldsAndParameters() {
        ReplicationHeader replication = new ReplicationHeader(Instant.parse("2026-09-04T20:21:02Z"), 4881L,
                "https://download.geofabrik.de/asia/thailand-updates");

        assertEquals("{\"operation\":\"load\",\"source_pbf\":\"thailand.osm.pbf\","
                        + "\"replication_timestamp\":\"2026-09-04T20:21:02Z\",\"replication_sequence\":4881,"
                        + "\"replication_url\":\"https://download.geofabrik.de/asia/thailand-updates\",\"parameters\":{}}",
                ImportState.json(ImportState.LOAD, new File("/data/thailand.osm.pbf"), replication, "{}"));
        assertEquals("{\"operation\":\"load\",\"source_pbf\":\"a.osm.pbf\",\"replication_timestamp\":null,"
                        + "\"replication_sequence\":null,\"replication_url\":null,\"parameters\":{\"x\":1}}",
                ImportState.json(ImportState.LOAD, new File("a.osm.pbf"), ReplicationHeader.NONE, "{\"x\":1}"));
    }
}
