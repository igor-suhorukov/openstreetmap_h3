package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.github.isuhorukov.osm.pgsnapshot.poi.MapcssMappingTest.resourceReader;
import static org.junit.jupiter.api.Assertions.*;

class CategoriesTest {

    private static Categories categories;
    private static TypeNames typeNames;

    @BeforeAll
    static void setUp() throws IOException {
        try (BufferedReader reader = resourceReader("/organicmaps/categories.txt")) {
            categories = Categories.parse(reader);
        }
        try (BufferedReader reader = resourceReader("/organicmaps/strings/types_strings.txt")) {
            typeNames = TypeNames.parse(reader, OrganicMapsPoiClassifier.NAME_LANGUAGES);
        }
    }

    @Test
    void categoriesOf_entryWithSeveralTypes_displayedGroupsOnly() {
        assertEquals(Optional.of(Set.of("category_transport")), categories.categoriesOf(List.of("railway", "halt")));
        assertEquals(Optional.of(Set.of("category_transport")), categories.categoriesOf(List.of("building", "train_station")));
        assertEquals(Optional.of(Set.of("category_eat", "category_food")), categories.categoriesOf(List.of("shop", "bakery")));
        assertEquals(Optional.of(Set.of()), categories.categoriesOf(List.of("amenity", "charging_station")));
    }

    @Test
    void categoriesOf_entryWithoutGroups_empty() {
        assertEquals(Optional.of(Set.of()), categories.categoriesOf(List.of("amenity", "school")));
    }

    @Test
    void categoriesOf_subtypeWithoutEntry_usesParentEntry() {
        assertEquals(Optional.of(Set.of("category_parking")),
                categories.categoriesOf(List.of("amenity", "parking", "underground", "fee")));
        assertEquals(Optional.of(Set.of()), categories.categoriesOf(List.of("tourism", "information", "board")));
    }

    @Test
    void categoriesOf_specificEntryPreferredToParent() {
        assertEquals(Optional.of(Set.of("category_transport")), categories.categoriesOf(List.of("railway", "station", "subway")));
    }

    @Test
    void categoriesOf_noEntry_absent() {
        assertEquals(Optional.empty(), categories.categoriesOf(List.of("railway", "rail")));
        assertEquals(Optional.empty(), categories.categoriesOf(List.of("railway", "subway_entrance")));
        assertEquals(Optional.empty(), categories.categoriesOf(List.of("building")));
    }

    @Test
    void name_typeSection_translationsOfRequestedLanguages() {
        assertEquals("Bus Stop", typeNames.name(List.of("highway", "bus_stop"), "en"));
        assertEquals("Остановка", typeNames.name(List.of("highway", "bus_stop"), "ru"));
        assertEquals("ป้ายรถเมล์", typeNames.name(List.of("highway", "bus_stop"), "th"));
        assertNull(typeNames.name(List.of("highway", "bus_stop"), "af"));
    }

    @Test
    void name_subtypeWithoutSection_usesParentName() {
        assertEquals("Train Station", typeNames.name(List.of("railway", "station", "subway"), "en"));
        assertNull(typeNames.name(List.of("railway", "rail"), "en"));
    }
}
