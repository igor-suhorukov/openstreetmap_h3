package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    @Test
    void rulesFile_hasOneIncludingLinePerType() throws IOException {
        Set<String> types = new HashSet<>();
        List<String> duplicates = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(PoiRules.class.getResourceAsStream(OrganicMapsPoiClassifier.RULES_RESOURCE)), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("+") && !types.add(line.split(" ")[0])) {
                    duplicates.add(line);
                }
            }
        }
        assertEquals(List.of(), duplicates);
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
    void exclusionConditions_typeAndSubtypes() throws IOException {
        PoiRules rules = parse("-leisure access=private;no\n-sport-snorkeling tourism=hotel;hostel\n-sport-snorkeling office=*\n+leisure-park\n");
        assertEquals(List.of(Map.of("access", Set.of("private", "no"))), rules.exclusionConditions("leisure-park-permissive"));
        assertEquals(List.of(Map.of("tourism", Set.of("hotel", "hostel")), Map.of("office", Set.of("*"))), rules.exclusionConditions("sport-snorkeling"));
        assertEquals(List.of(), rules.exclusionConditions("amenity-cafe"));
        assertFalse(rules.isExcluded("leisure-park"));
        assertFalse(rules.isExcluded("sport-snorkeling"));
        assertEquals(Set.of("leisure", "leisure-park", "sport-snorkeling"), rules.ruleTypes());
    }

    @Test
    void parse_exclusionWithEmptyCondition_throws() {
        assertThrows(IllegalArgumentException.class, () -> parse("-leisure access=\n"));
        assertThrows(IllegalArgumentException.class, () -> parse("-leisure =private\n"));
        assertThrows(IllegalArgumentException.class, () -> parse("-leisure access=private category_eat\n"));
    }

    @Test
    void exclusionRule_onlyForExcludedTypes() throws IOException {
        PoiRules rules = parse("-tourism-information\n+tourism-information-office\n");
        assertEquals(Optional.of("tourism-information"), rules.exclusionRule("tourism-information-board"));
        assertEquals(Optional.empty(), rules.exclusionRule("tourism-information-office"));
        assertEquals(Optional.of("tourism-information-office"), rules.inclusionRule("tourism-information-office"));
        assertEquals(Optional.empty(), rules.inclusionRule("tourism-information-board"));
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
    void removedCategories_mostSpecificRule() throws IOException {
        PoiRules rules = parse("+amenity-ferry_terminal category_sea_transport !category_transport name=*\n+amenity-ferry_terminal-big\n");
        assertEquals(Optional.of(Set.of("category_sea_transport")), rules.includedCategories("amenity-ferry_terminal"));
        assertEquals(Set.of("category_transport"), rules.removedCategories("amenity-ferry_terminal"));
        assertEquals(Set.of("category_transport"), rules.removedCategories("amenity-ferry_terminal-small"));
        assertEquals(Map.of("name", Set.of("*")), rules.tagConditions("amenity-ferry_terminal"));
        assertEquals(Set.of(), rules.removedCategories("amenity-ferry_terminal-big"));
        assertEquals(Set.of(), rules.removedCategories("amenity-cafe"));
    }

    @Test
    void parse_exclusionWithRemovedCategory_throws() {
        assertThrows(IllegalArgumentException.class, () -> parse("-amenity-ferry_terminal !category_transport\n"));
    }

    @Test
    void parse_signWithoutType_throws() {
        assertThrows(IllegalArgumentException.class, () -> parse("+\n"));
    }
}
