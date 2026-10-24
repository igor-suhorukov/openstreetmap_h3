package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.isuhorukov.osm.pgsnapshot.poi.MapcssMappingTest.resourceReader;
import static com.github.isuhorukov.osm.pgsnapshot.poi.MapcssMappingTest.tags;
import static org.junit.jupiter.api.Assertions.*;

class TagReplacerTest {

    private static TagReplacer tagReplacer;

    @BeforeAll
    static void setUp() throws IOException {
        try (BufferedReader reader = resourceReader("/organicmaps/replaced_tags.txt")) {
            tagReplacer = TagReplacer.parse(reader);
        }
    }

    private static List<String> apply(String... keyValues) {
        List<MutableTag> tags = tags(keyValues);
        tagReplacer.apply(tags);
        return tags.stream().map(tag -> tag.key + '=' + tag.value).collect(Collectors.toList());
    }

    @Test
    void apply_rule_originalTagReplaced() {
        assertEquals(List.of("name=Gelato", "amenity=ice_cream"), apply("shop=ice_cream", "name=Gelato"));
    }

    @Test
    void apply_updateRule_existingTagUpdated() {
        assertEquals(List.of("amenity=parcel_locker"), apply("amenity=vending_machine", "vending=parcel_pickup"));
    }

    @Test
    void apply_updateRule_missingTagAdded() {
        assertEquals(List.of("amenity=parcel_locker"), apply("vending=parcel_pickup"));
    }

    @Test
    void apply_sourceValueWithSemicolon_matchedAsWhole() {
        assertEquals(List.of("amenity=parcel_locker"), apply("vending=parcel_pickup;parcel_mail_in"));
        assertEquals(List.of("vending=parcel_mail_in"), apply("vending=parcel_mail_in"));
    }

    @Test
    void apply_noRule_tagsUnchanged() {
        assertEquals(List.of("shop=bakery", "atm=no"), apply("shop=bakery", "atm=no"));
    }

    @Test
    void replaces_onlyTagsWithRules() {
        assertTrue(tagReplacer.replaces("atm", "yes"));
        assertFalse(tagReplacer.replaces("atm", "no"));
        assertFalse(tagReplacer.replaces("amenity", "atm"));
    }

    @Test
    void parse_lineWithoutReplacementSeparator_throws() {
        BufferedReader reader = new BufferedReader(new StringReader("atm=yes amenity=atm\n"));
        assertThrows(IllegalArgumentException.class, () -> TagReplacer.parse(reader));
    }

    @Test
    void parse_duplicateSourceTag_throws() {
        BufferedReader reader = new BufferedReader(new StringReader("atm=yes : amenity=atm\natm=yes : amenity=bank\n"));
        assertThrows(IllegalArgumentException.class, () -> TagReplacer.parse(reader));
    }
}
