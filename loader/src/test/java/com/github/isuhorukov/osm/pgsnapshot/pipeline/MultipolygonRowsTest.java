package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MultipolygonRowsTest {

    private static final String OSMIUM_OUTPUT = "0106A\tway\t1\t\"amenity\"=>\"cafe\"\n"
            + "0106B\trelation\t2\t\"landuse\"=>\"forest\"\n"
            + "0106C\trelation\t3\t\"amenity\"=>\"school\"\n"
            + "0106D\trelation\t4\t\n"
            + "0106E\trelation\t5\t\"amenity\"=>\"cafe\",\"name\"=>\"a\\tb\"";

    private static final String ENRICHED_RELATIONS = "0106B\trelation\t2\t\"landuse\"=>\"forest\"\n"
            + "0106C\trelation\t3\t\"amenity\"=>\"school\",\"is.test\"=>\"v-Relation\"\n"
            + "0106D\trelation\t4\t\n"
            + "0106E\trelation\t5\t\"amenity\"=>\"cafe\",\"name\"=>\"a\\tb\",\"is.test\"=>\"v-Relation\"";

    private static final MultipolygonTags TEST_TAGS = MultipolygonTags.of(List.of(new TestTagExtension("test", "is.test", "v")));

    private static InputStream input(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_extensionTags_appendedToMatchingRelationRows() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        assertEquals(4, MultipolygonRows.copyRelations(input(OSMIUM_OUTPUT), output::write, TEST_TAGS));

        assertEquals(ENRICHED_RELATIONS, output.toString(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_extensionTagsInSmallReads_sameRows() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        InputStream smallReads = new FilterInputStream(input(OSMIUM_OUTPUT)) {
            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return super.read(buffer, offset, Math.min(length, 3));
            }
        };

        assertEquals(4, MultipolygonRows.copyRelations(smallReads, output::write, TEST_TAGS));

        assertEquals(ENRICHED_RELATIONS, output.toString(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_noMultipolygonTags_relationRowsUnchanged() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        assertEquals(4, MultipolygonRows.copyRelations(input(OSMIUM_OUTPUT), output::write, MultipolygonTags.NONE));

        assertEquals(OSMIUM_OUTPUT.substring(OSMIUM_OUTPUT.indexOf("0106B")), output.toString(StandardCharsets.UTF_8));
    }

    @Test
    void isRelation_secondColumnOnly() {
        assertTrue(isRelation("geom\trelation\t1\t\\N"));
        assertFalse(isRelation("geom\tway\t1\t\\N"));
        assertFalse(isRelation("geom\trelations\t1"));
        assertFalse(isRelation("geom\trelation"));
        assertFalse(isRelation("no tabs"));
        assertFalse(isRelation("a\tb\trelation\t"));
    }

    private static boolean isRelation(String line) {
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        return MultipolygonRows.isRelation(bytes, 0, bytes.length);
    }
}
