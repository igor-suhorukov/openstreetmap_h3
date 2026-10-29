package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.postgresql.copy.CopyIn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonFixtures.cell;
import static com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonFixtures.circle;
import static com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonFixtures.ewkbHex;
import static com.github.isuhorukov.osm.pgsnapshot.pipeline.MultipolygonFixtures.square;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MultipolygonCopyTest {

    private static final String CHIANG_MAI = ewkbHex(square(98.99, 18.79, 0.001));
    private static final String BANGKOK = ewkbHex(square(100.50, 13.75, 0.001));
    private static final String CHIANG_MAI_COLUMNS = "\t" + cell(18.79, 98.99) + "\t\\N\n";
    private static final String BANGKOK_COLUMNS = "\t" + cell(13.75, 100.50) + "\t\\N\n";
    private static final String OSMIUM_OUTPUT = CHIANG_MAI + "\tway\t1\t\"building\"=>\"yes\"\n"
            + CHIANG_MAI + "\trelation\t2\t\"landuse\"=>\"forest\"\n"
            + BANGKOK + "\trelation\t3\t\"type\"=>\"multipolygon\"\n"
            + CHIANG_MAI + "\tway\t4\t\"relation\"=>\"no\"\n"
            + CHIANG_MAI + "\trelation\t5\t\\N";
    private static final String COPIED_ROWS = CHIANG_MAI + "\trelation\t2\t\"landuse\"=>\"forest\"" + CHIANG_MAI_COLUMNS
            + BANGKOK + "\trelation\t3\t\"type\"=>\"multipolygon\"" + BANGKOK_COLUMNS
            + CHIANG_MAI + "\trelation\t5\t\\N" + CHIANG_MAI_COLUMNS;

    private final ByteArrayOutputStream copied = new ByteArrayOutputStream();
    private CopyIn copyIn;

    @BeforeEach
    void mockCopyIn() throws Exception {
        copyIn = mock(CopyIn.class);
        doAnswer(invocation -> {
            copied.write(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2));
            return null;
        }).when(copyIn).writeToCopy(any(byte[].class), anyInt(), anyInt());
    }

    private static InputStream input(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private static InputStream smallReads(String content) {
        return new FilterInputStream(input(content)) {
            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return super.read(buffer, offset, Math.min(length, 3));
            }
        };
    }

    @Test
    void copyRelations_onlyRelationRowsCopiedWithCells() throws Exception {
        assertEquals(3, MultipolygonCopy.copyRelations(input(OSMIUM_OUTPUT), copyIn, MultipolygonTags.NONE, 0));

        assertEquals(COPIED_ROWS, copied.toString(StandardCharsets.UTF_8));
        verify(copyIn, times(1)).writeToCopy(any(byte[].class), anyInt(), anyInt());
    }

    @Test
    void copyRelations_smallReads_sameRows() throws Exception {
        assertEquals(3, MultipolygonCopy.copyRelations(smallReads(OSMIUM_OUTPUT), copyIn, MultipolygonTags.NONE, 2));

        assertEquals(COPIED_ROWS, copied.toString(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_rowLongerThanBuffer_copiedWhole() throws Exception {
        String hugePolygon = ewkbHex(circle(98.9905, 18.7905, 0.0005, 100_000)) + "\trelation\t7\t\\N";

        assertEquals(1, MultipolygonCopy.copyRelations(input(CHIANG_MAI + "\tway\t6\t\\N\n" + hugePolygon + "\n"), copyIn,
                MultipolygonTags.NONE, 2));

        assertEquals(hugePolygon + CHIANG_MAI_COLUMNS, copied.toString(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_hugeRowInPipeSizedReads_linearTime() {
        String hugePolygon = ewkbHex(circle(98.9905, 18.7905, 0.0005, 1 << 19)) + "\trelation\t8\t\\N";
        InputStream pipe = new FilterInputStream(input(hugePolygon)) {
            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return super.read(buffer, offset, Math.min(length, 4096));
            }
        };

        assertEquals(1, assertTimeoutPreemptively(Duration.ofSeconds(20),
                () -> MultipolygonCopy.copyRelations(pipe, copyIn, MultipolygonTags.NONE, 2)));
        assertEquals(hugePolygon.length() + CHIANG_MAI_COLUMNS.length(), copied.size());
    }

    @Test
    void copyRelations_emptyInput_noRows() throws Exception {
        assertEquals(0, MultipolygonCopy.copyRelations(input(""), copyIn, MultipolygonTags.NONE, 2));

        verify(copyIn, never()).writeToCopy(any(byte[].class), anyInt(), anyInt());
    }

    @Test
    void copyRelations_multipolygonTags_appendedInCopiedRows() throws Exception {
        TagExtension landuse = new TagExtension() {
            @Override
            public String getName() {
                return "landuse";
            }

            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public List<Tag> additionalTags(Entity entity) {
                return entity.getTags().stream().anyMatch(tag -> "landuse".equals(tag.getKey()))
                        ? List.of(new Tag("is.landuse", "{" + entity.getId() + "}")) : List.of();
            }

            @Override
            public Set<String> multipolygonTagKeys() {
                return Set.of("is.landuse");
            }
        };

        assertEquals(3, MultipolygonCopy.copyRelations(input(OSMIUM_OUTPUT), copyIn, MultipolygonTags.of(List.of(landuse)), 2));

        assertEquals(CHIANG_MAI + "\trelation\t2\t\"landuse\"=>\"forest\",\"is.landuse\"=>\"{2}\"" + CHIANG_MAI_COLUMNS
                + BANGKOK + "\trelation\t3\t\"type\"=>\"multipolygon\"" + BANGKOK_COLUMNS
                + CHIANG_MAI + "\trelation\t5\t\\N" + CHIANG_MAI_COLUMNS, copied.toString(StandardCharsets.UTF_8));
    }
}
