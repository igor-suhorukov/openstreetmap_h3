package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.isuhorukov.osm.pgsnapshot.poi.OrganicMapsPoiClassifier.POI_CATEGORY_TAG;
import static com.github.isuhorukov.osm.pgsnapshot.poi.OrganicMapsPoiClassifier.POI_TAG;
import static org.junit.jupiter.api.Assertions.*;

class OrganicMapsPoiClassifierTest {

    private static OrganicMapsPoiClassifier classifier;

    @BeforeAll
    static void setUp() throws Exception {
        classifier = OrganicMapsPoiClassifier.load(testDataDirectory());
    }

    static Path testDataDirectory() throws Exception {
        return Paths.get(Objects.requireNonNull(OrganicMapsPoiClassifierTest.class.getResource("/organicmaps")).toURI());
    }

    static List<Tag> tags(String... keyValues) {
        List<Tag> tags = new ArrayList<>();
        for (String keyValue : keyValues) {
            int separator = keyValue.indexOf('=');
            tags.add(new Tag(keyValue.substring(0, separator), keyValue.substring(separator + 1)));
        }
        return tags;
    }

    private static Map<String, String> poiTags(String... keyValues) {
        return classifier.poiTags(tags(keyValues)).stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    @Test
    void poiTags_busStop_typeAndTransportCategory() {
        assertEquals(Map.of(POI_TAG, "{highway-bus_stop}", POI_CATEGORY_TAG, "{category_transport}"),
                poiTags("highway=bus_stop", "public_transport=platform", "name=Victory Monument"));
    }

    @Test
    void poiTags_streetFurniture_notPoi() {
        assertTrue(poiTags("amenity=bench").isEmpty());
        assertTrue(poiTags("amenity=parking", "fee=yes").isEmpty());
        assertTrue(poiTags("amenity=parking", "location=underground", "fee=yes").isEmpty());
    }

    @Test
    void poiTags_typeWithoutDisplayedCategory_onlyPoiTag() {
        assertEquals(Map.of(POI_TAG, "{amenity-school}"), poiTags("amenity=school", "building=yes"));
    }

    @Test
    void poiTags_replacedTag_classifiedAsReplacement() {
        assertEquals(Map.of(POI_TAG, "{amenity-ice_cream}"), poiTags("shop=ice_cream"));
        assertEquals(Map.of(POI_TAG, "{amenity-parcel_locker}", POI_CATEGORY_TAG, "{category_post}"),
                poiTags("amenity=vending_machine", "vending=parcel_pickup"));
        assertTrue(poiTags("amenity=vending_machine", "vending=drinks").isEmpty());
    }

    @Test
    void poiTags_subwayEntranceIncludedByProjectRule() {
        assertEquals(Map.of(POI_TAG, "{railway-subway_entrance}", POI_CATEGORY_TAG, "{category_transport}"),
                poiTags("railway=subway_entrance"));
    }

    @Test
    void poiTags_poiClassTypeWithoutCategoriesEntry_notPoi() {
        assertTrue(poiTags("railway=rail", "usage=main").isEmpty());
    }

    @Test
    void poiTags_typeOutsidePoiClasses_notPoi() {
        assertTrue(poiTags("highway=residential").isEmpty());
        assertTrue(poiTags("building=yes").isEmpty());
    }

    @Test
    void poiTags_twoLevelPoiType() {
        assertEquals(Map.of(POI_TAG, "{building-train_station}", POI_CATEGORY_TAG, "{category_transport}"),
                poiTags("building=train_station"));
    }

    @Test
    void poiTags_severalPoiTypes_allTypesAndCategories() {
        assertEquals(Map.of(POI_TAG, "{amenity-cafe,shop-bakery}", POI_CATEGORY_TAG, "{category_eat,category_food}"),
                poiTags("shop=bakery", "amenity=cafe"));
    }

    @Test
    void poiTags_tagsAreTrimmedAndEmptyIgnored() {
        assertEquals(Map.of(POI_TAG, "{amenity-cafe}", POI_CATEGORY_TAG, "{category_eat}"),
                poiTags(" amenity = cafe ", "name="));
    }

    @Test
    void poiTags_noTags_empty() {
        assertTrue(classifier.poiTags(Collections.emptyList()).isEmpty());
        assertTrue(classifier.poiTags(null).isEmpty());
    }

    @Test
    void poiTypes_namesSortedByName() {
        assertEquals(List.of("amenity-cafe", "shop-bakery"), classifier.poiTypes(tags("shop=bakery", "amenity=cafe")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=bench")));
    }

    @Test
    void getPoiTypes_catalogOfPoiTypesWithNames() {
        Map<String, PoiType> catalog = classifier.getPoiTypes().stream()
                .collect(Collectors.toMap(PoiType::getType, poiType -> poiType));

        PoiType busStop = catalog.get("highway-bus_stop");
        assertEquals(List.of("category_transport"), busStop.getCategories());
        assertEquals("Bus Stop", busStop.getName("en"));
        assertEquals("Остановка", busStop.getName("ru"));
        assertEquals("ป้ายรถเมล์", busStop.getName("th"));

        assertEquals(List.of(), catalog.get("amenity-school").getCategories());
        assertEquals("Subway Entrance", catalog.get("railway-subway_entrance").getName("en"));
        assertNull(catalog.get("amenity-ice_cream").getName("en"));
        assertFalse(catalog.containsKey("amenity-bench"));
        assertFalse(catalog.containsKey("amenity-parking-fee"));
        assertFalse(catalog.containsKey("railway-rail"));
        assertFalse(catalog.containsKey("highway-residential"));

        List<String> types = classifier.getPoiTypes().stream().map(PoiType::getType).collect(Collectors.toList());
        List<String> sorted = new ArrayList<>(types);
        Collections.sort(sorted);
        assertEquals(sorted, types);
    }

    @Test
    void getUnknownRuleTypes_rulesWithoutTypesInMapping() {
        Set<String> unknown = classifier.getUnknownRuleTypes();
        assertTrue(unknown.contains("emergency-fire_hydrant"));
        assertFalse(unknown.contains("amenity-bench"));
        assertFalse(unknown.contains("railway-subway_entrance"));
    }

    @Test
    void toArrayLiteral_quotesSpecialElements() {
        assertEquals("{}", OrganicMapsPoiClassifier.toArrayLiteral(List.of()));
        assertEquals("{a,b-c}", OrganicMapsPoiClassifier.toArrayLiteral(List.of("a", "b-c")));
        assertEquals("{\"a b\",\"x,y\",\"q\\\"\",\"NULL\",\"\"}",
                OrganicMapsPoiClassifier.toArrayLiteral(List.of("a b", "x,y", "q\"", "NULL", "")));
    }

    @Test
    void load_missingDirectory_throws() {
        assertThrows(NoSuchFileException.class, () -> OrganicMapsPoiClassifier.load(Paths.get("no_such_organicmaps_data")));
    }
}
