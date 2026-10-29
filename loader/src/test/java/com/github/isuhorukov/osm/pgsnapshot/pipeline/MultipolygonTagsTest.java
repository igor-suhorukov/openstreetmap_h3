package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MultipolygonTagsTest {

    private static final String OSMIUM_ESCAPED_ROW = "0106\trelation\t100\t\"amenity\"=>\"school\","
            + "\"name\"=>\"a \\\\\"q\\\\\" b\\\\\\\\s tab\\tnl\\ncr\\rend\",\"k=>x\"=>\"v,w\",\"name:th\"=>\"โรงเรียน\"";

    private static TagExtension extension(Set<String> multipolygonTagKeys, Function<Entity, List<Tag>> additionalTags) {
        return new TagExtension() {
            @Override
            public String getName() {
                return "extension";
            }

            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public List<Tag> additionalTags(Entity entity) {
                return additionalTags.apply(entity);
            }

            @Override
            public Set<String> multipolygonTagKeys() {
                return multipolygonTagKeys;
            }
        };
    }

    private static TagExtension adding(String key, String value) {
        return extension(Set.of(key), entity -> List.of(new Tag(key, value)));
    }

    private static Entity capturedRelation(String row) {
        List<Entity> entities = new ArrayList<>();
        assertNull(enriched(MultipolygonTags.of(List.of(extension(Set.of("is.a"), entity -> {
            entities.add(entity);
            return Collections.emptyList();
        }))), row));
        return entities.get(0);
    }

    private static List<String> pairs(Collection<Tag> tags) {
        return tags.stream().map(tag -> tag.getKey() + '=' + tag.getValue()).collect(Collectors.toList());
    }

    private static String enriched(MultipolygonTags multipolygonTags, String row) {
        byte[] bytes = row.getBytes(StandardCharsets.UTF_8);
        byte[] tags = multipolygonTags.enrichedTags(bytes, 0, bytes.length);
        return tags.length == 0 ? null : new String(tags, StandardCharsets.UTF_8);
    }

    @Test
    void of_noExtensionWithMultipolygonKeys_none() {
        TagExtension withoutKeys = extension(Collections.emptySet(), entity -> List.of(new Tag("is.a", "1")));

        assertSame(MultipolygonTags.NONE, MultipolygonTags.of(List.of()));
        assertSame(MultipolygonTags.NONE, MultipolygonTags.of(List.of(withoutKeys)));
        assertTrue(MultipolygonTags.NONE.isEmpty());
        assertFalse(MultipolygonTags.of(List.of(withoutKeys, adding("is.b", "2"))).isEmpty());
    }

    @Test
    void enrichedTags_relationWithAmenity_testExtensionTagAppended() {
        MultipolygonTags multipolygonTags = MultipolygonTags.of(List.of(new TestTagExtension("test", "is.test", "v")));

        assertEquals("\"amenity\"=>\"school\",\"is.test\"=>\"v-Relation\"",
                enriched(multipolygonTags, "0106\trelation\t5\t\"amenity\"=>\"school\""));
        assertNull(enriched(multipolygonTags, "0106\trelation\t6\t\"landuse\"=>\"forest\""));
    }

    @Test
    void enrichedTags_osmiumEscapes_relationEntityWithUnescapedTags() {
        Entity relation = capturedRelation(OSMIUM_ESCAPED_ROW);

        assertEquals(EntityType.Relation, relation.getType());
        assertEquals(100, relation.getId());
        assertEquals(List.of("amenity=school", "name=a \"q\" b\\s tab\tnl\ncr\rend", "k=>x=v,w", "name:th=โรงเรียน"),
                pairs(relation.getTags()));
    }

    @Test
    void enrichedTags_specialCharactersInValue_escapedAsOsmiumDoes() {
        MultipolygonTags multipolygonTags = MultipolygonTags.of(List.of(adding("is.a", "a \"q\" b\\s tab\tnl\ncr\rend")));

        String tags = enriched(multipolygonTags, "0106\trelation\t7\t\"amenity\"=>\"school\"");

        assertEquals("\"amenity\"=>\"school\",\"is.a\"=>\"a \\\\\"q\\\\\" b\\\\\\\\s tab\\tnl\\ncr\\rend\"", tags);
        assertEquals(List.of("amenity=school", "is.a=a \"q\" b\\s tab\tnl\ncr\rend"),
                pairs(capturedRelation("0106\trelation\t7\t" + tags).getTags()));
    }

    @Test
    void enrichedTags_emptyOrNullTags_pairsWithoutSeparator() {
        MultipolygonTags multipolygonTags = MultipolygonTags.of(List.of(adding("is.a", "1"), adding("is.b", "2")));

        assertEquals("\"is.a\"=>\"1\",\"is.b\"=>\"2\"", enriched(multipolygonTags, "0106\trelation\t8\t"));
        assertEquals("\"is.a\"=>\"1\",\"is.b\"=>\"2\"", enriched(multipolygonTags, "0106\trelation\t9\t\\N"));
    }

    @Test
    void enrichedTags_onlyMultipolygonKeysOfExtension() {
        MultipolygonTags multipolygonTags = MultipolygonTags.of(List.of(extension(Set.of("is.poi"),
                entity -> List.of(new Tag("is.poi", "{amenity-school}"), new Tag("is.other", "x")))));

        assertEquals("\"amenity\"=>\"school\",\"is.poi\"=>\"{amenity-school}\"",
                enriched(multipolygonTags, "0106\trelation\t10\t\"amenity\"=>\"school\""));
    }

    @Test
    void enrichedTags_nullValueAndSpaces_skipped() {
        Entity relation = capturedRelation("0106\trelation\t11\t\"a\"=>NULL, \"b\" => \"c\"");

        assertEquals(List.of("b=c"), pairs(relation.getTags()));
    }

    @Test
    void tags_relationWithAmenity_osmiumTagsThenExtensionTags() {
        MultipolygonTags multipolygonTags = MultipolygonTags.of(List.of(new TestTagExtension("test", "is.test", "v")));

        assertEquals(List.of("amenity=school", "name=a \"q\" b\\s tab\tnl\ncr\rend", "k=>x=v,w", "name:th=โรงเรียน",
                "is.test=v-Relation"), pairs(tags(multipolygonTags, OSMIUM_ESCAPED_ROW)));
        assertEquals(List.of("landuse=forest"), pairs(tags(multipolygonTags, "0106\trelation\t6\t\"landuse\"=>\"forest\"")));
    }

    @Test
    void tags_noTagsOrNoExtensions_osmiumTagsOnly() {
        assertEquals(List.of(), pairs(tags(MultipolygonTags.NONE, "0106\trelation\t8\t")));
        assertEquals(List.of(), pairs(tags(MultipolygonTags.NONE, "0106\trelation\t9\t\\N")));
        assertEquals(List.of("is.a=1"), pairs(tags(MultipolygonTags.of(List.of(adding("is.a", "1"))), "0106\trelation\t8\t")));
        assertEquals(List.of("amenity=school"), pairs(tags(MultipolygonTags.NONE, "0106\trelation\t5\t\"amenity\"=>\"school\"")));
    }

    @Test
    void relationId_osmiumRow_idColumn() {
        byte[] row = OSMIUM_ESCAPED_ROW.getBytes(StandardCharsets.UTF_8);

        assertEquals(100, MultipolygonTags.relationId(row, 0, row.length));
    }

    private static List<Tag> tags(MultipolygonTags multipolygonTags, String row) {
        byte[] bytes = row.getBytes(StandardCharsets.UTF_8);
        return multipolygonTags.tags(bytes, 0, bytes.length);
    }

    @Test
    void enrichedTags_malformedRow_throws() {
        MultipolygonTags multipolygonTags = MultipolygonTags.of(List.of(adding("is.a", "1")));

        IllegalStateException malformedTags = assertThrows(IllegalStateException.class,
                () -> enriched(multipolygonTags, "0106\trelation\t12\t\"amenity\"=>school"));
        assertTrue(malformedTags.getMessage().contains("relation 12"), malformedTags.getMessage());
        assertThrows(IllegalStateException.class, () -> enriched(multipolygonTags, "0106\trelation\tx\t\"a\"=>\"b\""));
        assertThrows(IllegalStateException.class, () -> enriched(multipolygonTags, "no tabs"));
    }
}
