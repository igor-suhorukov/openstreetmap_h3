package com.github.isuhorukov.osm.pgsnapshot.poi;

import com.beust.jcommander.JCommander;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.*;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.isuhorukov.osm.pgsnapshot.poi.OrganicMapsPoiClassifier.POI_CATEGORY_TAG;
import static com.github.isuhorukov.osm.pgsnapshot.poi.OrganicMapsPoiClassifier.POI_TAG;
import static org.junit.jupiter.api.Assertions.*;

class OrganicMapsPoiExtensionTest {

    @TempDir
    Path tempDir;

    private static OrganicMapsPoiExtension parse(String... args) {
        OrganicMapsPoiExtension extension = new OrganicMapsPoiExtension();
        JCommander.newBuilder().addObject(extension.getParameters()).build().parse(args);
        return extension;
    }

    private static OrganicMapsPoiExtension initializedExtension() throws Exception {
        OrganicMapsPoiExtension extension = parse("-poi_mapping_dir", OrganicMapsPoiClassifierTest.testDataDirectory().toString());
        extension.init();
        return extension;
    }

    private static Node node(Tag... tags) {
        CommonEntityData data = new CommonEntityData(1L, 1, new Date(), OsmUser.NONE, 1L);
        data.getTags().addAll(Arrays.asList(tags));
        return new Node(data, 13.7563, 100.5018);
    }

    @Test
    void serviceLoader_findsExtension() {
        List<TagExtension> extensions = new ArrayList<>();
        ServiceLoader.load(TagExtension.class).forEach(extensions::add);
        assertTrue(extensions.stream().anyMatch(OrganicMapsPoiExtension.class::isInstance), extensions.toString());
    }

    @Test
    void isEnabled_onlyWithMappingDirectory() throws Exception {
        assertFalse(parse().isEnabled());
        assertTrue(parse("-poi_mapping_dir", OrganicMapsPoiClassifierTest.testDataDirectory().toString()).isEnabled());
    }

    @Test
    void additionalTags_poiTypeTagsOfEntity() throws Exception {
        OrganicMapsPoiExtension extension = initializedExtension();

        Map<String, String> tags = extension.additionalTags(node(new Tag("highway", "bus_stop"))).stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
        assertEquals(Map.of(POI_TAG, "{highway-bus_stop}", POI_CATEGORY_TAG, "{category_transport}"), tags);
        assertTrue(extension.additionalTags(node(new Tag("amenity", "bench"))).isEmpty());
        assertTrue(extension.additionalTags(node()).isEmpty());
    }

    @Test
    void additionalTags_beforeInit_throws() throws Exception {
        OrganicMapsPoiExtension extension = parse("-poi_mapping_dir", OrganicMapsPoiClassifierTest.testDataDirectory().toString());
        Node busStop = node(new Tag("highway", "bus_stop"));
        assertThrows(IllegalStateException.class, () -> extension.additionalTags(busStop));
    }

    @Test
    void init_missingDirectory_throws() {
        OrganicMapsPoiExtension extension = parse("-poi_mapping_dir", tempDir.resolve("no_such_dir").toString());
        assertThrows(NoSuchFileException.class, extension::init);
    }

    @Test
    void getFingerprint_sha256OfRuleFiles() throws Exception {
        String fingerprint = initializedExtension().getFingerprint();

        assertTrue(fingerprint.matches("[0-9a-f]{64}"), fingerprint);
        assertEquals(fingerprint, initializedExtension().getFingerprint());
        assertEquals(fingerprint, RulesFingerprint.of(OrganicMapsPoiClassifierTest.testDataDirectory()));
    }

    @Test
    void getFingerprint_changedOrMissingRuleFile_differs() throws Exception {
        Path data = Files.createDirectories(tempDir.resolve("data"));
        Path source = OrganicMapsPoiClassifierTest.testDataDirectory();
        for (String file : List.of(OrganicMapsPoiClassifier.MAPPING_FILE, OrganicMapsPoiClassifier.REPLACED_TAGS_FILE,
                OrganicMapsPoiClassifier.CATEGORIES_FILE)) {
            Files.copy(source.resolve(file), data.resolve(file));
        }
        String withoutTypeNames = RulesFingerprint.of(data);
        Files.writeString(data.resolve(OrganicMapsPoiClassifier.CATEGORIES_FILE), "\n", StandardOpenOption.APPEND);

        assertNotEquals(RulesFingerprint.of(source), withoutTypeNames);
        assertNotEquals(withoutTypeNames, RulesFingerprint.of(data));
    }

    @Test
    void getFingerprint_beforeInit_throws() {
        OrganicMapsPoiExtension extension = new OrganicMapsPoiExtension();

        assertThrows(IllegalStateException.class, extension::getFingerprint);
    }

    @Test
    void hex_lowercaseTwoDigitsPerByte() {
        assertEquals("00ff7f80", RulesFingerprint.hex(new byte[]{0, -1, 127, -128}));
    }

    @Test
    void multipolygonTagKeys_poiTags() {
        assertEquals(Set.of(POI_TAG, POI_CATEGORY_TAG), new OrganicMapsPoiExtension().multipolygonTagKeys());
    }

    @Test
    void writePostgresqlResources_catalogAndScript() throws Exception {
        RecordingPostgresqlResources resources = new RecordingPostgresqlResources();

        initializedExtension().writePostgresqlResources(resources);

        assertTrue(resources.staticFiles.get(PoiTypeCatalogWriter.CATALOG_FILE).length > 0);
        assertTrue(resources.importScripts.containsKey(PoiTypeCatalogWriter.CATALOG_SCRIPT));
    }
}
