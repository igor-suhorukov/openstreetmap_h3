package com.github.isuhorukov.osm.pgsnapshot.extension;

import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    @Test
    void defaults_noOptionsNoMultipolygonTagsNoResources() throws IOException {
        List<String> resources = new ArrayList<>();

        assertNull(MINIMAL_EXTENSION.getParameters());
        assertEquals("", MINIMAL_EXTENSION.getFingerprint());
        assertTrue(MINIMAL_EXTENSION.multipolygonTagKeys().isEmpty());
        assertDoesNotThrow(MINIMAL_EXTENSION::init);
        MINIMAL_EXTENSION.writePostgresqlResources(new PostgresqlResources() {
            @Override
            public void addStaticFile(String name, byte[] content) {
                resources.add(name);
            }

            @Override
            public void addImportScript(String name, String script) {
                resources.add(name);
            }
        });

        assertTrue(resources.isEmpty());
    }
}
