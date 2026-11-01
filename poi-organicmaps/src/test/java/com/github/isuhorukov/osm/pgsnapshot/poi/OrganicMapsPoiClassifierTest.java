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
    }

    @Test
    void poiTags_paediatricsAndCourses_projectTypes() {
        assertEquals(Map.of(POI_TAG, "{healthcare-paediatrics}", POI_CATEGORY_TAG, "{category_paediatrics}"), poiTags("healthcare:speciality=paediatrics"));
        assertEquals(Map.of(POI_TAG, "{healthcare-paediatrics}", POI_CATEGORY_TAG, "{category_paediatrics}"), poiTags("healthcare:speciality=surgery;paediatrics"));
        assertEquals(Map.of(POI_TAG, "{healthcare-paediatrics}", POI_CATEGORY_TAG, "{category_paediatrics}"), poiTags("provided_for:child=yes"));
        assertEquals(Map.of(POI_TAG, "{amenity-training}"), poiTags("amenity=training"));
        assertEquals(Map.of(POI_TAG, "{amenity-prep_school}"), poiTags("amenity=prep_school"));
    }

    @Test
    void poiTags_mccHalt_metroCategory() {
        assertEquals(Map.of(POI_TAG, "{railway-halt-mcc}", POI_CATEGORY_TAG, "{category_metro,category_transport}"),
                poiTags("railway=halt", "network=МЦК"));
    }

    @Test
    void poiTags_animalPlaces_petsCategory() {
        Map<String, String> vet = Map.of(POI_TAG, "{amenity-veterinary}", POI_CATEGORY_TAG, "{category_pets}");
        assertEquals(vet, poiTags("amenity=veterinary"));
        assertEquals(Map.of(POI_TAG, "{leisure-dog_park}", POI_CATEGORY_TAG, "{category_pets}"), poiTags("leisure=dog_park"));
        assertEquals(Map.of(POI_TAG, "{shop-pet}", POI_CATEGORY_TAG, "{category_pets}"), poiTags("shop=pet"));
        assertEquals(Map.of(POI_TAG, "{amenity-animal_shelter}", POI_CATEGORY_TAG, "{category_pets}"), poiTags("amenity=animal_shelter"));
        assertEquals(Map.of(POI_TAG, "{amenity-animal_boarding}", POI_CATEGORY_TAG, "{category_pets}"), poiTags("amenity=animal_boarding"));
        assertEquals(Map.of(POI_TAG, "{amenity-animal_breeding}", POI_CATEGORY_TAG, "{category_pets}"), poiTags("amenity=animal_breeding"));
        assertEquals(Map.of(POI_TAG, "{amenity-animal_training}", POI_CATEGORY_TAG, "{category_pets}"), poiTags("amenity=animal_training"));
    }

    @Test
    void poiTags_parkingAndAtm_ownCategories() {
        assertEquals(Map.of(POI_TAG, "{amenity-parking}", POI_CATEGORY_TAG, "{category_parking}"), poiTags("amenity=parking"));
        assertEquals(Map.of(POI_TAG, "{amenity-parking-fee}", POI_CATEGORY_TAG, "{category_parking}"), poiTags("amenity=parking", "fee=yes"));
        assertEquals(Map.of(POI_TAG, "{amenity-parking-underground-fee}", POI_CATEGORY_TAG, "{category_parking}"),
                poiTags("amenity=parking", "location=underground", "fee=yes"));
        assertEquals(Map.of(POI_TAG, "{amenity-atm}", POI_CATEGORY_TAG, "{category_atm}"), poiTags("amenity=atm"));
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
        assertEquals(Map.of(POI_TAG, "{railway-subway_entrance}", POI_CATEGORY_TAG, "{category_metro,category_transport}"),
                poiTags("railway=subway_entrance"));
    }

    @Test
    void poiTags_typesIncludedByProjectRules_withoutCategory() {
        assertEquals(Map.of(POI_TAG, "{man_made-bridge}"), poiTags("man_made=bridge"));
        assertEquals(Map.of(POI_TAG, "{place-square}"), poiTags("place=square"));
        assertEquals(Map.of(POI_TAG, "{landuse-vineyard}"), poiTags("landuse=vineyard"));
        assertEquals(Map.of(POI_TAG, "{landuse-orchard}"), poiTags("landuse=orchard"));
    }

    @Test
    void poiTags_divingObjects_diveCategory() {
        Map<String, String> site = Map.of(POI_TAG, "{sport-scuba_diving}", POI_CATEGORY_TAG, "{category_diving}");
        assertEquals(site, poiTags("sport=scuba_diving"));
        assertEquals(Map.of(POI_TAG, "{amenity-dive_centre}", POI_CATEGORY_TAG, "{category_diving}"),
                poiTags("amenity=dive_centre"));
        assertEquals(Map.of(POI_TAG, "{shop-scuba_diving}", POI_CATEGORY_TAG, "{category_diving}"),
                poiTags("shop=scuba_diving"));
        assertEquals(Map.of(), poiTags("scuba_diving:divespot=no"));
    }

    @Test
    void poiTags_diveSites_diveSiteCategory() {
        Map<String, String> site = Map.of(POI_TAG, "{sport-scuba_diving-divespot}", POI_CATEGORY_TAG, "{category_dive_site,category_diving}");
        assertEquals(site, poiTags("scuba_diving:divespot=yes"));
        assertEquals(site, poiTags("sport=scuba_diving", "scuba_diving:entry:boat=yes"));
        assertEquals(Map.of(POI_TAG, "{natural-reef,sport-scuba_diving-divespot}", POI_CATEGORY_TAG, "{category_dive_site,category_diving}"),
                poiTags("sport=scuba_diving", "natural=reef"));
        assertEquals(Map.of(POI_TAG, "{sport-scuba_diving}", POI_CATEGORY_TAG, "{category_diving}"),
                poiTags("sport=scuba_diving", "leisure=swimming_area"));
        assertEquals(Map.of(POI_TAG, "{sport-snorkeling}", POI_CATEGORY_TAG, "{category_dive_site}"),
                poiTags("scuba_diving:type:snorkeling=yes"));
        assertEquals(Map.of(POI_TAG, "{natural-reef}", POI_CATEGORY_TAG, "{category_dive_site}"), poiTags("natural=reef"));
        assertEquals(Map.of(POI_TAG, "{sport-freediving}", POI_CATEGORY_TAG, "{category_diving}"), poiTags("sport=freediving"));
    }

    @Test
    void poiTags_aerialwayStation_ownCategory() {
        assertEquals(Map.of(POI_TAG, "{aerialway-station}", POI_CATEGORY_TAG, "{category_aerialway}"),
                poiTags("aerialway=station"));
    }

    @Test
    void poiTags_plainWater_drinkingWaterSubcategory() {
        assertEquals(Map.of(POI_TAG, "{amenity-drinking_water}", POI_CATEGORY_TAG, "{category_drinking_water,category_water}"),
                poiTags("amenity=drinking_water"));
        assertEquals(Map.of(POI_TAG, "{man_made-water_tap}", POI_CATEGORY_TAG, "{category_drinking_water,category_water}"),
                poiTags("man_made=water_tap"));
        assertEquals(Map.of(POI_TAG, "{man_made-water_well}", POI_CATEGORY_TAG, "{category_drinking_water,category_water}"),
                poiTags("man_made=water_well"));
        assertEquals(Map.of(POI_TAG, "{amenity-water_point}", POI_CATEGORY_TAG, "{category_drinking_water,category_rv,category_water}"),
                poiTags("amenity=water_point"));
    }

    @Test
    void poiTags_notDrinkableTapWellOrWaterPoint_notPoi() {
        assertEquals(Map.of(), poiTags("man_made=water_tap", "drinking_water=no"));
        assertEquals(Map.of(), poiTags("man_made=water_well", "drinking_water=no"));
        assertEquals(Map.of(), poiTags("amenity=water_point", "drinking_water=no"));
    }

    @Test
    void poiTags_springsAndGeysers_springsSubcategory() {
        assertEquals(Map.of(POI_TAG, "{natural-spring}", POI_CATEGORY_TAG, "{category_springs,category_water}"),
                poiTags("natural=spring"));
        assertEquals(Map.of(POI_TAG, "{natural-spring-drinking_water_no}", POI_CATEGORY_TAG, "{category_springs,category_water}"),
                poiTags("natural=spring", "drinking_water=no"));
        assertEquals(Map.of(POI_TAG, "{natural-hot_spring}", POI_CATEGORY_TAG, "{category_springs,category_water}"),
                poiTags("natural=hot_spring"));
        assertEquals(Map.of(POI_TAG, "{natural-geyser}", POI_CATEGORY_TAG, "{category_springs}"),
                poiTags("natural=geyser"));
    }

    @Test
    void poiTags_beachesAndPeaks_ownCategories() {
        assertEquals(Map.of(POI_TAG, "{natural-beach}", POI_CATEGORY_TAG, "{category_beach}"), poiTags("natural=beach"));
        assertEquals(Map.of(POI_TAG, "{natural-beach-sand}", POI_CATEGORY_TAG, "{category_beach}"),
                poiTags("natural=beach", "surface=sand"));
        assertEquals(Map.of(POI_TAG, "{leisure-beach_resort}", POI_CATEGORY_TAG, "{category_beach}"),
                poiTags("leisure=beach_resort"));
        assertEquals(Map.of(POI_TAG, "{natural-peak}", POI_CATEGORY_TAG, "{category_peak}"), poiTags("natural=peak"));
    }

    @Test
    void poiTags_volcanoes_ownCategory() {
        assertEquals(Map.of(POI_TAG, "{natural-volcano}", POI_CATEGORY_TAG, "{category_volcano}"), poiTags("natural=volcano"));
        assertEquals(Map.of(POI_TAG, "{natural-volcano-active}", POI_CATEGORY_TAG, "{category_volcano}"),
                poiTags("natural=volcano", "volcano:status=active"));
    }

    @Test
    void poiTags_coastAndNatureTypes_poiByProjectRules() {
        assertEquals(Map.of(POI_TAG, "{man_made-pier}"), poiTags("man_made=pier"));
        assertEquals(Map.of(POI_TAG, "{place-island}"), poiTags("place=island"));
        assertEquals(Map.of(POI_TAG, "{place-islet}"), poiTags("place=islet"));
        assertEquals(Map.of(POI_TAG, "{natural-bay}"), poiTags("natural=bay"));
        assertEquals(Map.of(POI_TAG, "{natural-cape}"), poiTags("natural=cape"));
        assertEquals(Map.of(), poiTags("natural=cliff"));
        assertEquals(Map.of(POI_TAG, "{natural-cliff-notable}"), poiTags("natural=cliff", "name=Aiguille"));
        assertEquals(Map.of(POI_TAG, "{natural-cliff-notable}"), poiTags("natural=cliff", "climbing=crag"));
        assertEquals(Map.of(POI_TAG, "{natural-cliff-notable}"), poiTags("natural=cliff", "sport=climbing"));
        assertEquals(Map.of(POI_TAG, "{leisure-ice_rink}"), poiTags("leisure=ice_rink"));
        assertEquals(Map.of(POI_TAG, "{boundary-national_park}"), poiTags("boundary=national_park"));
    }

    @Test
    void poiTags_projectLeisureTypes_poiByProjectMapping() {
        assertEquals(Map.of(POI_TAG, "{leisure-fishing}"), poiTags("leisure=fishing"));
        assertEquals(Map.of(POI_TAG, "{amenity-coworking_space}"), poiTags("amenity=coworking_space"));
        assertEquals(Map.of(POI_TAG, "{amenity-coworking_space}"), poiTags("office=coworking"));
        assertEquals(Map.of(POI_TAG, "{leisure-horse_riding}"), poiTags("leisure=horse_riding"));
        assertEquals(Map.of(POI_TAG, "{leisure-spa}"), poiTags("leisure=spa"));
        assertEquals(Map.of(POI_TAG, "{leisure-spa}"), poiTags("amenity=spa"));
        assertEquals(Map.of(POI_TAG, "{leisure-spa}"), poiTags("tourism=spa_resort"));
    }

    @Test
    void poiTags_severalValuesOfOneTag_everyValueMatched() {
        assertEquals(Map.of(POI_TAG, "{natural-peak,natural-volcano}", POI_CATEGORY_TAG, "{category_peak,category_volcano}"),
                poiTags("natural=peak;volcano"));
        assertEquals(Map.of(POI_TAG, "{natural-peak,natural-volcano}", POI_CATEGORY_TAG, "{category_peak,category_volcano}"),
                poiTags("natural= peak ; volcano ;"));
        assertEquals(Map.of(POI_TAG, "{amenity-cafe}", POI_CATEGORY_TAG, "{category_eat}"), poiTags("amenity=bench;cafe"));
        assertEquals(Map.of(), poiTags("amenity=bench;post_box"));
        assertEquals(Map.of(), poiTags("amenity=;"));
    }

    @Test
    void poiTags_severalValuesOfConditionTag_anyValueSatisfies() {
        assertEquals(Map.of(POI_TAG, "{man_made-tower}"), poiTags("man_made=tower", "tower:type=water;observation"));
        assertEquals(Map.of(), poiTags("man_made=tower", "tower:type=water;defensive"));
    }

    @Test
    void getPoiTypes_projectTypes_namesFromProjectFile() {
        Map<String, PoiType> catalog = classifier.getPoiTypes().stream()
                .collect(Collectors.toMap(PoiType::getType, poiType -> poiType));

        PoiType diveCentre = catalog.get("amenity-dive_centre");
        assertEquals(List.of("category_diving"), diveCentre.getCategories());
        assertEquals("Dive Centre", diveCentre.getName("en"));
        assertEquals("Дайв-центр", diveCentre.getName("ru"));
        assertEquals("ศูนย์ดำน้ำ", diveCentre.getName("th"));
        assertEquals("Scuba Diving Shop", catalog.get("shop-scuba_diving").getName("en"));
        assertEquals("Место для рыбалки", catalog.get("leisure-fishing").getName("ru"));
        assertEquals("Coworking Space", catalog.get("amenity-coworking_space").getName("en"));
        assertEquals("Верховая езда", catalog.get("leisure-horse_riding").getName("ru"));
        assertEquals("สปา", catalog.get("leisure-spa").getName("th"));
    }

    @Test
    void poiTags_towers_onlyObservationBellTowerPagodaStupaAndChedi() {
        assertEquals(Map.of(POI_TAG, "{man_made-tower}"), poiTags("man_made=tower", "tower:type=observation"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}"), poiTags("man_made=tower", "tower:type=bell_tower"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}"), poiTags("man_made=tower", "tower:type=pagoda"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}"), poiTags("man_made=tower", "tower:type=stupa"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}"), poiTags("man_made=tower", "tower:type=chedi"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}"), poiTags("man_made=tower", "tower:type=lighting; observation"));
        assertTrue(poiTags("man_made=tower").isEmpty());
        assertTrue(poiTags("man_made=tower", "tower:type=lighting").isEmpty());
        assertTrue(poiTags("man_made=tower", "tower:type=cooling").isEmpty());
        assertTrue(poiTags("man_made=tower", "tower:type=communication").isEmpty());
        assertTrue(poiTags("man_made=mast", "tower:type=communication").isEmpty());
    }

    @Test
    void poiTags_towerOfOtherTypeWithPoiTag_keepsOnlyThatType() {
        assertEquals(Map.of(POI_TAG, "{amenity-cafe}", POI_CATEGORY_TAG, "{category_eat}"),
                poiTags("man_made=tower", "tower:type=lighting", "amenity=cafe"));
        assertEquals(Map.of(POI_TAG, "{amenity-cafe,man_made-tower}", POI_CATEGORY_TAG, "{category_eat}"),
                poiTags("man_made=tower", "tower:type=observation", "amenity=cafe"));
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
