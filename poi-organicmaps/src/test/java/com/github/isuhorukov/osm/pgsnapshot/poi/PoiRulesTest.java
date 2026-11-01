package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PoiRulesTest {

    @Test
    void isExcluded_moreSpecificInclusionWins() throws Exception {
        PoiRules rules = parse("-tourism-information\n+tourism-information-office\n+amenity-parking\n-amenity-parking-private");
        assertTrue(rules.isExcluded("tourism-information"));
        assertTrue(rules.isExcluded("tourism-information-board"));
        org.junit.jupiter.api.Assertions.assertFalse(rules.isExcluded("tourism-information-office"));
        org.junit.jupiter.api.Assertions.assertFalse(rules.isExcluded("amenity-parking"));
        assertTrue(rules.isExcluded("amenity-parking-private"));
    }

    private static PoiRules parse(String text) throws IOException {
        return PoiRules.parse(new BufferedReader(new StringReader(text)));
    }

    @Test
    void isExcluded_typeAndSubtypes() throws IOException {
        PoiRules rules = parse("# comment\n-amenity-parking\n");
        assertTrue(rules.isExcluded("amenity-parking"));
        assertTrue(rules.isExcluded("amenity-parking-underground-fee"));
        assertFalse(rules.isExcluded("amenity-parking_entrance"));
        assertFalse(rules.isExcluded("amenity"));
    }

    @Test
    void includedCategories_mostSpecificRule() throws IOException {
        PoiRules rules = parse("+railway-subway_entrance category_transport\n+railway-subway_entrance-bangkok\n");
        assertEquals(Optional.of(Set.of("category_transport")), rules.includedCategories("railway-subway_entrance-london"));
        assertEquals(Optional.of(Set.of()), rules.includedCategories("railway-subway_entrance-bangkok"));
        assertEquals(Optional.empty(), rules.includedCategories("railway-station"));
        assertEquals(Set.of("railway-subway_entrance", "railway-subway_entrance-bangkok"), rules.ruleTypes());
    }

    @Test
    void tagConditions_mostSpecificRule() throws IOException {
        PoiRules rules = parse("+man_made-tower category_tourism tower:type=observation;bell_tower\n+man_made-tower-big\n");
        assertEquals(Map.of("tower:type", Set.of("observation", "bell_tower")), rules.tagConditions("man_made-tower"));
        assertEquals(Map.of("tower:type", Set.of("observation", "bell_tower")), rules.tagConditions("man_made-tower-small"));
        assertEquals(Optional.of(Set.of("category_tourism")), rules.includedCategories("man_made-tower"));
        assertEquals(Map.of(), rules.tagConditions("man_made-tower-big"));
        assertEquals(Map.of(), rules.tagConditions("amenity-cafe"));
    }

    @Test
    void parse_conditionWithoutKeyOrValue_throws() {
        assertThrows(IllegalArgumentException.class, () -> parse("+man_made-tower tower:type=\n"));
        assertThrows(IllegalArgumentException.class, () -> parse("+man_made-tower =observation\n"));
    }

    @Test
    void parse_ruleWithoutSign_throws() {
        assertThrows(IllegalArgumentException.class, () -> parse("amenity-bench\n"));
    }

    @Test
    void parse_exclusionWithCategory_throws() {
        assertThrows(IllegalArgumentException.class, () -> parse("-amenity-bench category_eat\n"));
    }

    @Test
    void parse_signWithoutType_throws() {
        assertThrows(IllegalArgumentException.class, () -> parse("+\n"));
    }
}
