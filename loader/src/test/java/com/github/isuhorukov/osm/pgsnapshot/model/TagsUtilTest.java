package com.github.isuhorukov.osm.pgsnapshot.model;

import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TagsUtilTest {

    @Test
    void tagsToMap_null_returnsEmptyMap() {
        assertTrue(TagsUtil.tagsToMap(null).isEmpty());
    }

    @Test
    void tagsToMap_empty_returnsEmptyMap() {
        assertTrue(TagsUtil.tagsToMap(Collections.emptyList()).isEmpty());
    }

    @Test
    void tagsToMap_validTags_returnsMappedEntries() {
        Map<String, String> result = TagsUtil.tagsToMap(
                List.of(new Tag("name", "Main Street"), new Tag("highway", "primary")));
        assertEquals("Main Street", result.get("name"));
        assertEquals("primary", result.get("highway"));
    }

    @Test
    void tagsToMap_tagWithBlankKey_filtered() {
        Map<String, String> result = TagsUtil.tagsToMap(
                List.of(new Tag("  ", "value"), new Tag("name", "ok")));
        assertFalse(result.containsKey("  "));
        assertEquals("ok", result.get("name"));
    }

    @Test
    void tagsToMap_tagWithBlankValue_filtered() {
        Map<String, String> result = TagsUtil.tagsToMap(
                List.of(new Tag("name", "  "), new Tag("highway", "road")));
        assertFalse(result.containsKey("name"));
        assertEquals("road", result.get("highway"));
    }

    @Test
    void tagsToMap_resultIsSorted() {
        Map<String, String> result = TagsUtil.tagsToMap(
                List.of(new Tag("z_key", "z"), new Tag("a_key", "a")));
        // TreeMap preserves insertion with natural order
        assertEquals("a_key", result.keySet().iterator().next());
    }
}
