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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MultipolygonCopyTest {

    private static final String OSMIUM_OUTPUT = "0106A\tway\t1\t\"building\"=>\"yes\"\n"
            + "0106B\trelation\t2\t\"landuse\"=>\"forest\"\n"
            + "0106C\trelation\t3\t\"type\"=>\"multipolygon\"\n"
            + "0106D\tway\t4\t\"relation\"=>\"no\"\n"
            + "0106E\trelation\t5\t\\N";

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
    void copyRelations_onlyRelationRowsCopied() throws Exception {
        assertEquals(3, MultipolygonCopy.copyRelations(input(OSMIUM_OUTPUT), copyIn, MultipolygonTags.NONE, 0));

        assertEquals("0106B\trelation\t2\t\"landuse\"=>\"forest\"\n0106C\trelation\t3\t\"type\"=>\"multipolygon\"\n"
                + "0106E\trelation\t5\t\\N", copied.toString(StandardCharsets.UTF_8));
        verify(copyIn, times(1)).writeToCopy(any(byte[].class), anyInt(), anyInt());
    }

    @Test
    void copyRelations_smallReads_sameRows() throws Exception {
        assertEquals(3, MultipolygonCopy.copyRelations(smallReads(OSMIUM_OUTPUT), copyIn, MultipolygonTags.NONE, 2));

        assertEquals("0106B\trelation\t2\t\"landuse\"=>\"forest\"\n0106C\trelation\t3\t\"type\"=>\"multipolygon\"\n"
                + "0106E\trelation\t5\t\\N", copied.toString(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_rowLongerThanBuffer_copiedWhole() throws Exception {
        String hugePolygon = "01".repeat(1_500_000) + "\trelation\t7\t\\N\n";

        assertEquals(1, MultipolygonCopy.copyRelations(input("0106\tway\t6\t\\N\n" + hugePolygon), copyIn, MultipolygonTags.NONE, 2));

        assertEquals(hugePolygon, copied.toString(StandardCharsets.UTF_8));
    }

    @Test
    void copyRelations_hugeRowInPipeSizedReads_linearTime() {
        String hugePolygon = "0".repeat(16 << 20) + "\trelation\t8\t\\N\n";
        InputStream pipe = new FilterInputStream(input(hugePolygon)) {
            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                return super.read(buffer, offset, Math.min(length, 4096));
            }
        };

        assertEquals(1, assertTimeoutPreemptively(Duration.ofSeconds(20),
                () -> MultipolygonCopy.copyRelations(pipe, copyIn, MultipolygonTags.NONE, 2)));
        assertEquals(hugePolygon.length(), copied.size());
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

        assertEquals("0106B\trelation\t2\t\"landuse\"=>\"forest\",\"is.landuse\"=>\"{2}\"\n"
                + "0106C\trelation\t3\t\"type\"=>\"multipolygon\"\n0106E\trelation\t5\t\\N", copied.toString(StandardCharsets.UTF_8));
    }
}
