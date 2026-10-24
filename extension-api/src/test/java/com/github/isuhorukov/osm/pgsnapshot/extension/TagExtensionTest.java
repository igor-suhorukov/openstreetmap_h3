package com.github.isuhorukov.osm.pgsnapshot.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TagExtensionTest {

    private static final TagExtension MINIMAL_EXTENSION = new TagExtension() {
        @Override
        public String getName() {
            return "minimal";
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public List<Tag> additionalTags(Entity entity) {
            return Collections.emptyList();
        }
    };

    @TempDir
    Path tempDir;

    @Test
    void defaults_noOptionsNoMultipolygonTagsNoResources() throws IOException {
        File directory = tempDir.toFile();

        assertNull(MINIMAL_EXTENSION.getParameters());
        assertTrue(MINIMAL_EXTENSION.multipolygonTagKeys().isEmpty());
        assertDoesNotThrow(MINIMAL_EXTENSION::init);
        MINIMAL_EXTENSION.writePostgresqlResources(directory, directory);

        try (Stream<Path> files = Files.list(tempDir)) {
            assertEquals(0, files.count());
        }
    }
}
