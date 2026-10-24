package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MapcssMappingTest {

    private static MapcssMapping mapping;

    @BeforeAll
    static void setUp() throws IOException {
        try (BufferedReader reader = resourceReader("/organicmaps/mapcss-mapping.csv")) {
            mapping = new MapcssMapping(MapcssMapping.parseRules(reader));
        }
    }

    static BufferedReader resourceReader(String resource) {
        return new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(MapcssMappingTest.class.getResourceAsStream(resource)), StandardCharsets.UTF_8));
    }

    static List<MutableTag> tags(String... keyValues) {
        List<MutableTag> tags = new ArrayList<>();
        for (String keyValue : keyValues) {
            int separator = keyValue.indexOf('=');
            tags.add(new MutableTag(keyValue.substring(0, separator), keyValue.substring(separator + 1)));
        }
        return tags;
    }

    private static List<String> match(String... keyValues) {
        return mapping.match(tags(keyValues)).stream().map(MapcssMapping::typeName).collect(Collectors.toList());
    }

    @Test
    void match_shortFormat_matchesTagOfTypeName() {
        assertEquals(List.of("highway-bus_stop"), match("highway=bus_stop", "name=Central"));
    }

    @Test
    void match_unknownTags_empty() {
        assertTrue(match("name=Central", "surface=asphalt").isEmpty());
    }

    @Test
    void match_mandatoryKey_anyValueExceptNo() {
        assertEquals(List.of("amenity-parking-fee"), match("amenity=parking", "fee=yes"));
        assertEquals(List.of("amenity-parking-fee"), match("amenity=parking", "fee=20 THB"));
        assertEquals(List.of("amenity-parking"), match("amenity=parking", "fee=no"));
    }

    @Test
    void match_forbiddenKey_missingOrNo() {
        assertEquals(List.of("railway-rail-main"), match("railway=rail", "usage=main"));
        assertEquals(List.of("railway-rail-main"), match("railway=rail", "usage=main", "highspeed=no"));
        assertEquals(List.of("railway-rail"), match("railway=rail", "usage=main", "highspeed=yes"));
    }

    @Test
    void match_alternativeSelectors_eachOneMatches() {
        assertEquals(List.of("railway-station-subway"), match("railway=station", "station=subway"));
        assertEquals(List.of("railway-station-subway"), match("railway=station", "transport=subway"));
    }

    @Test
    void match_longerTypeOfSameGroup_removesShorterOnes() {
        assertEquals(List.of("amenity-parking-underground-fee"),
                match("amenity=parking", "location=underground", "fee=yes"));
    }

    @Test
    void match_equalArityTypesOfSameGroup_bothKept() {
        assertEquals(List.of("amenity-charging_station-bicycle", "amenity-charging_station-motorcar"),
                match("amenity=charging_station", "motorcar=yes", "bicycle=yes"));
    }

    @Test
    void match_oneComponentType_removedByTypeWithSameFirstComponent() {
        assertEquals(List.of("building-train_station"), match("building=train_station"));
        assertEquals(List.of("building"), match("building=yes"));
    }

    @Test
    void match_differentGroups_allKept() {
        assertEquals(List.of("amenity-cafe", "shop-bakery"), match("amenity=cafe", "shop=bakery"));
    }

    @Test
    void parseRules_deprecatedRows_skipped() {
        assertTrue(match("highway=track", "tracktype=grade2").isEmpty());
        assertTrue(match("railway=yard").isEmpty());
    }

    @Test
    void parseRules_wrongFieldCount_throws() {
        BufferedReader reader = new BufferedReader(new StringReader("amenity|cafe;108\n"));
        assertThrows(IllegalArgumentException.class, () -> MapcssMapping.parseRules(reader));
    }

    @Test
    void parseRules_shortFormatWithThreeLevelType_throws() {
        BufferedReader reader = new BufferedReader(new StringReader("amenity|parking|fee;125;\n"));
        assertThrows(IllegalArgumentException.class, () -> MapcssMapping.parseRules(reader));
    }

    @Test
    void isIndexed_firstConditionOfRules() {
        assertTrue(mapping.isIndexed("highway", "bus_stop"));
        assertTrue(mapping.isIndexed("building", "anything"));
        assertFalse(mapping.isIndexed("highway", "primary"));
        assertFalse(mapping.hasUnindexedRules());
    }
}
