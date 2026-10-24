package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Paths;
import java.util.List;

import static com.github.isuhorukov.osm.pgsnapshot.poi.OrganicMapsPoiClassifierTest.tags;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = "organicmaps.data.dir", matches = ".+")
class OrganicMapsPoiClassifierDataTest {

    private static OrganicMapsPoiClassifier classifier;

    @BeforeAll
    static void setUp() throws Exception {
        classifier = OrganicMapsPoiClassifier.load(Paths.get(System.getProperty("organicmaps.data.dir")));
    }

    @Test
    void projectRules_referToExistingTypes() {
        assertEquals(List.of(), List.copyOf(classifier.getUnknownRuleTypes()));
    }

    @Test
    void poiTypes_commonObjects() {
        assertEquals(List.of("highway-bus_stop"), classifier.poiTypes(tags("highway=bus_stop", "public_transport=platform")));
        assertEquals(List.of("shop-supermarket"), classifier.poiTypes(tags("shop=supermarket", "building=retail")));
        assertEquals(List.of("amenity-restaurant"), classifier.poiTypes(tags("amenity=restaurant", "cuisine=thai")));
        assertEquals(List.of("amenity-school"), classifier.poiTypes(tags("amenity=school")));
        assertEquals(List.of("railway-station-subway"), classifier.poiTypes(tags("railway=station", "station=subway")));
        assertEquals(List.of("railway-subway_entrance"), classifier.poiTypes(tags("railway=subway_entrance")));
    }

    @Test
    void poiTypes_notPoi() {
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=bench")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=parking", "fee=yes")));
        assertEquals(List.of(), classifier.poiTypes(tags("emergency=fire_hydrant")));
        assertEquals(List.of(), classifier.poiTypes(tags("railway=rail")));
        assertEquals(List.of(), classifier.poiTypes(tags("railway=level_crossing")));
        assertEquals(List.of(), classifier.poiTypes(tags("highway=primary", "name=Sukhumvit")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=house")));
    }

    @Test
    void getPoiTypes_displayedCategoriesAndNames() {
        PoiType supermarket = classifier.getPoiTypes().stream()
                .filter(poiType -> poiType.getType().equals("shop-supermarket")).findFirst().orElseThrow();
        assertEquals(List.of("category_food"), supermarket.getCategories());
        assertEquals("Supermarket", supermarket.getName("en"));
        assertNotNull(supermarket.getName("ru"));
        assertNotNull(supermarket.getName("th"));
        assertTrue(classifier.getPoiTypes().size() > 300);
    }
}
