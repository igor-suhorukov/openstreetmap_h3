package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
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

    @Test
    void poiTags_protectedArea_onlyNamedClasses235() {
        assertEquals(Map.of(POI_TAG, "{boundary-protected_area-2}", POI_CATEGORY_TAG, "{category_protected_area}"), poiTags("boundary=protected_area", "protect_class=2", "name=Park"));
        assertEquals(Map.of(POI_TAG, "{boundary-protected_area-3}", POI_CATEGORY_TAG, "{category_protected_area}"), poiTags("boundary=protected_area", "protect_class=3", "name=Park"));
        assertEquals(Map.of(POI_TAG, "{boundary-protected_area-5}", POI_CATEGORY_TAG, "{category_protected_area}"), poiTags("boundary=protected_area", "protect_class=5", "name=Park"));
        assertTrue(poiTags("boundary=protected_area", "protect_class=2").isEmpty());
        assertTrue(poiTags("boundary=protected_area", "protect_class=4", "name=Park").isEmpty());
        assertTrue(poiTags("boundary=protected_area", "name=Park").isEmpty());
    }

    @Test
    void poiTags_moreCrafts_categoryOrServices() {
        Map<String, String> categories = Map.ofEntries(Map.entry("oil_mill", "category_craft_food"), Map.entry("coffee_roaster", "category_craft_food"),
                Map.entry("cooper", "category_craft_drink"), Map.entry("printmaker", "category_craft_art"), Map.entry("stained_glass", "category_craft_art"),
                Map.entry("glassblower", "category_craft_art"), Map.entry("embroiderer", "category_crafts"), Map.entry("bookbinder", "category_crafts"),
                Map.entry("leather", "category_crafts"), Map.entry("candlemaker", "category_crafts"), Map.entry("soap", "category_crafts"),
                Map.entry("musical_instrument", "category_craft_music"));
        categories.forEach((craft, category) -> assertEquals(Map.of(POI_TAG, "{craft-" + craft + "}", POI_CATEGORY_TAG, "{" + category + "}"), poiTags("craft=" + craft)));
        assertEquals(Map.of(POI_TAG, "{craft-atelier}", POI_CATEGORY_TAG, "{category_craft_art}"), poiTags("craft=atelier", "name=Studio"));
        assertTrue(poiTags("craft=atelier").isEmpty());
        assertTrue(poiTags("craft=sculptor").isEmpty());
        for (String craft : List.of("engraver", "saddler", "boatbuilder")) {
            assertEquals(Map.of(POI_TAG, "{craft-" + craft + "}", POI_CATEGORY_TAG, "{category_services}"), poiTags("craft=" + craft));
        }
    }

    @Test
    void poiTags_craftsAndGastronomy_ownCategory() {
        assertEquals(Map.of(POI_TAG, "{craft-pottery}", POI_CATEGORY_TAG, "{category_crafts}"), poiTags("craft=pottery"));
        assertEquals(Map.of(POI_TAG, "{craft-distillery}", POI_CATEGORY_TAG, "{category_craft_drink}"), poiTags("craft=distillery"));
        assertEquals(Map.of(POI_TAG, "{craft-cheese}", POI_CATEGORY_TAG, "{category_craft_food}"), poiTags("craft=cheese"));
        assertEquals(Map.of(POI_TAG, "{craft-bakery}", POI_CATEGORY_TAG, "{category_food_makers}"), poiTags("craft=bakery"));
        assertEquals(Map.of(POI_TAG, "{craft-jeweller}", POI_CATEGORY_TAG, "{category_services}"), poiTags("craft=jeweller"));
        assertTrue(poiTags("craft=stonemason").isEmpty());
        assertEquals(Map.of(POI_TAG, "{craft-luthier}", POI_CATEGORY_TAG, "{category_craft_music}"), poiTags("craft=luthier"));
        assertEquals(Map.of(POI_TAG, "{tourism-wine_cellar}", POI_CATEGORY_TAG, "{category_craft_drink}"), poiTags("tourism=wine_cellar"));
        assertEquals(Map.of(POI_TAG, "{amenity-cooking_school}", POI_CATEGORY_TAG, "{category_craft_food}"), poiTags("amenity=cooking_school"));
    }

    @Test
    void poiTags_offices_workCategoryAndSubcategory() {
        assertEquals(work("office-company", "category_office"), poiTags("office=company"));
        assertEquals(work("office", "category_office"), poiTags("office=it"));
        assertEquals(work("office", "category_office"), poiTags("office=yes"));
        assertEquals(work("office-lawyer", "category_office_services"), poiTags("office=notary"));
        assertEquals(work("office-accountant", "category_office_services"), poiTags("office=accountant"));
        assertEquals(work("office-visa", "category_office_services"), poiTags("office=visa"));
        assertEquals(work("office-government", "category_government"), poiTags("office=administrative"));
        assertEquals(work("office-diplomatic", "category_government"), poiTags("amenity=embassy"));
        assertEquals(work("amenity-townhall", "category_government"), poiTags("amenity=townhall"));
        assertEquals(work("amenity-courthouse", "category_government"), poiTags("amenity=courthouse"));
        assertEquals(work("office-ngo", "category_ngo"), poiTags("office=ngo"));
        assertEquals(work("office-association", "category_ngo"), poiTags("office=association"));
        assertEquals(work("office-political_party", "category_ngo"), poiTags("office=political_party"));
        assertEquals(work("office-research", "category_research"), poiTags("office=research"));
        assertEquals(work("amenity-research_institute", "category_research"), poiTags("amenity=research_institute"));
        assertEquals(work("amenity-coworking_space,office-coworking", "category_coworking"), poiTags("office=coworking"));
    }

    @Test
    void poiTags_businessCentresAndOfficeBuildings_onlyWithName() {
        assertEquals(work("building-office", "category_business_centre"), poiTags("building=office", "name=Tower"));
        assertTrue(poiTags("building=office").isEmpty());
        assertTrue(poiTags("building=commercial", "name=Tower").isEmpty());
        assertEquals(work("amenity-business_centre", "category_business_centre"), poiTags("amenity=business_center"));
        assertEquals(Map.of(POI_TAG, "{building-office,office-company}", POI_CATEGORY_TAG, "{category_business_centre,category_office,category_work}"),
                poiTags("building=office", "office=company", "name=Tower"));
        assertEquals(work("amenity-courthouse", "category_government"), poiTags("building=office", "amenity=courthouse", "name=Court"));
        assertEquals(work("landuse-commercial", "category_commercial_area"), poiTags("landuse=commercial", "name=Business Park"));
        assertTrue(poiTags("landuse=commercial").isEmpty());
        assertEquals(Map.of(POI_TAG, "{amenity-cafe}", POI_CATEGORY_TAG, "{category_eat}"), poiTags("landuse=commercial", "amenity=cafe", "name=Cafe"));
    }

    @Test
    void poiTags_universitiesAndColleges_categoryAndNamedBuildings() {
        assertEquals(Map.of(POI_TAG, "{amenity-university}", POI_CATEGORY_TAG, "{category_college_university,category_university}"), poiTags("amenity=university"));
        assertEquals(Map.of(POI_TAG, "{amenity-college}", POI_CATEGORY_TAG, "{category_college,category_college_university}"), poiTags("amenity=college", "name=Техникум"));
        assertEquals(Map.of(POI_TAG, "{building-university}", POI_CATEGORY_TAG, "{category_college_university,category_university}"), poiTags("building=university", "name=Main Hall"));
        assertEquals(Map.of(POI_TAG, "{building-college}", POI_CATEGORY_TAG, "{category_college,category_college_university}"), poiTags("building=college", "name=Workshop"));
        assertTrue(poiTags("building=university").isEmpty());
        assertTrue(poiTags("building=college").isEmpty());
        assertEquals(Map.of(POI_TAG, "{amenity-university}", POI_CATEGORY_TAG, "{category_college_university,category_university}"), poiTags("building=university", "amenity=university", "name=Campus"));
        assertEquals(Map.of(POI_TAG, "{amenity-cafe}", POI_CATEGORY_TAG, "{category_eat}"), poiTags("building=college", "amenity=cafe", "name=Cafe"));
    }

    @Test
    void poiTags_industryWarehousesDepots_areasOnlyWithName() {
        assertEquals(work("man_made-works", "category_industrial"), poiTags("man_made=works"));
        assertEquals(work("landuse-industrial", "category_industrial"), poiTags("landuse=industrial", "name=Plant"));
        assertEquals(work("landuse-industrial-mine", "category_industrial"), poiTags("landuse=industrial", "industrial=mine", "name=Mine"));
        assertTrue(poiTags("landuse=industrial").isEmpty());
        assertEquals(work("landuse-industrial-warehouse", "category_warehouse"), poiTags("landuse=industrial", "industrial=warehouse", "name=Stock"));
        assertEquals(work("landuse-industrial-warehouse", "category_warehouse"), poiTags("landuse=industrial", "industrial=logistics", "name=Hub"));
        assertTrue(poiTags("landuse=industrial", "industrial=warehouse").isEmpty());
        assertEquals(work("landuse-industrial-depot", "category_depot"), poiTags("landuse=industrial", "industrial=depot", "name=Bus Depot"));
        assertTrue(poiTags("landuse=industrial", "industrial=depot").isEmpty());
        assertEquals(work("amenity-research_institute", "category_research"), poiTags("landuse=industrial", "amenity=research_institute", "name=Institute"));
    }

    private static Map<String, String> work(String types, String category) {
        return Map.of(POI_TAG, "{" + types + "}", POI_CATEGORY_TAG, "{" + category + ",category_work}");
    }

    @Test
    void poiTags_geotourism_ownCategory() {
        Map<String, String> rock = Map.of(POI_TAG, "{natural-rock}", POI_CATEGORY_TAG, "{category_geotourism}");
        assertEquals(rock, poiTags("natural=stone", "name=Pierre"));
        assertEquals(rock, poiTags("natural=rock", "name=Pierre"));
        assertTrue(poiTags("natural=stone").isEmpty());
        assertEquals(Map.of(POI_TAG, "{natural-arch}", POI_CATEGORY_TAG, "{category_geotourism}"), poiTags("natural=arch"));
        assertEquals(Map.of(POI_TAG, "{natural-cave_entrance}", POI_CATEGORY_TAG, "{category_geotourism}"), poiTags("natural=cave_entrance"));
        assertEquals(Map.of(POI_TAG, "{natural-gorge}", POI_CATEGORY_TAG, "{category_geotourism}"), poiTags("natural=gorge"));
        assertEquals(Map.of(POI_TAG, "{natural-sinkhole}", POI_CATEGORY_TAG, "{category_geotourism}"), poiTags("natural=sinkhole", "name=Aven"));
        assertTrue(poiTags("natural=sinkhole").isEmpty());
        assertEquals(Map.of(POI_TAG, "{natural-valley}", POI_CATEGORY_TAG, "{category_geotourism}"), poiTags("natural=valley", "name=Vallon"));
        assertTrue(poiTags("natural=valley").isEmpty());
        assertEquals(Map.of(POI_TAG, "{natural-ridge}", POI_CATEGORY_TAG, "{category_geotourism}"), poiTags("natural=ridge", "name=Crete"));
        assertTrue(poiTags("natural=ridge").isEmpty());
        assertEquals(Map.of(POI_TAG, "{geological-palaeontological_site}", POI_CATEGORY_TAG, "{category_geotourism}"), poiTags("geological=palaeontological_site"));
        assertEquals(Map.of(POI_TAG, "{geological-glacial_erratic,natural-rock}", POI_CATEGORY_TAG, "{category_geotourism}"),
                poiTags("geological=glacial_erratic", "natural=stone", "name=Pierre"));
        assertTrue(poiTags("geological=glacial_erratic", "natural=stone").isEmpty());
    }

    @Test
    void poiTags_lavenderField_notPrivate() {
        Map<String, String> field = Map.of(POI_TAG, "{landuse-farmland-lavender}", POI_CATEGORY_TAG, "{category_lavender}");
        assertEquals(field, poiTags("landuse=farmland", "crop=lavender"));
        assertEquals(field, poiTags("landuse=farmland", "crop=lavandin"));
        assertEquals(field, poiTags("landuse=farmland", "crop=lavander"));
        assertEquals(field, poiTags("landuse=farmland", "crop=wheat;lavender"));
        assertEquals(field, poiTags("landuse=farmland", "crop=lavender", "access=yes"));
        assertTrue(poiTags("landuse=farmland", "crop=lavender", "access=private").isEmpty());
        assertTrue(poiTags("landuse=farmland", "crop=lavandin", "access=no").isEmpty());
        assertTrue(poiTags("landuse=farmland", "crop=wheat").isEmpty());
        assertTrue(poiTags("landuse=farmland").isEmpty());
    }

    @Test
    void poiTags_conditionOnAnyValueOfTheTag() {
        assertEquals(Map.of(POI_TAG, "{natural-glacier}", POI_CATEGORY_TAG, "{category_mountain}"), poiTags("natural=glacier", "name=Mer de Glace"));
        assertTrue(poiTags("natural=glacier").isEmpty());
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
        assertEquals(Map.of(POI_TAG, "{healthcare-trauma}", POI_CATEGORY_TAG, "{category_trauma}"), poiTags("healthcare:speciality=trauma"));
        assertEquals(Map.of(POI_TAG, "{healthcare-trauma}", POI_CATEGORY_TAG, "{category_trauma}"), poiTags("healthcare:speciality=surgery;trauma"));
        assertTrue(poiTags("healthcare:speciality=orthopaedics").isEmpty());
        assertEquals(Map.of(POI_TAG, "{emergency-emergency_ward_entrance}"), poiTags("emergency=emergency_ward_entrance"));
        assertEquals(Map.of(POI_TAG, "{amenity-training}", POI_CATEGORY_TAG, "{category_courses}"), poiTags("amenity=training"));
        assertEquals(Map.of(POI_TAG, "{amenity-prep_school}", POI_CATEGORY_TAG, "{category_courses}"), poiTags("amenity=prep_school"));
    }

    @Test
    void poiTags_mccHalt_metroCategory() {
        assertEquals(Map.of(POI_TAG, "{railway-halt-mcc}", POI_CATEGORY_TAG, "{category_metro,category_transport}"),
                poiTags("railway=halt", "network=МЦК"));
    }

    @Test
    void poiTags_animalPlaces_petsCategory() {
        Map<String, String> vet = Map.of(POI_TAG, "{amenity-veterinary}", POI_CATEGORY_TAG, "{category_pet_services,category_pets}");
        assertEquals(vet, poiTags("amenity=veterinary"));
        assertEquals(Map.of(POI_TAG, "{leisure-dog_park}", POI_CATEGORY_TAG, "{category_dog_areas,category_pets}"), poiTags("leisure=dog_park"));
        assertEquals(Map.of(POI_TAG, "{shop-pet}", POI_CATEGORY_TAG, "{category_pet_services,category_pets}"), poiTags("shop=pet"));
        assertEquals(Map.of(POI_TAG, "{amenity-animal_shelter}", POI_CATEGORY_TAG, "{category_pet_services,category_pets}"), poiTags("amenity=animal_shelter"));
        assertEquals(Map.of(POI_TAG, "{amenity-animal_boarding}", POI_CATEGORY_TAG, "{category_pet_services,category_pets}"), poiTags("amenity=animal_boarding"));
        assertEquals(Map.of(POI_TAG, "{amenity-animal_breeding}", POI_CATEGORY_TAG, "{category_pet_services,category_pets}"), poiTags("amenity=animal_breeding"));
        assertEquals(Map.of(POI_TAG, "{amenity-animal_training}", POI_CATEGORY_TAG, "{category_dog_areas,category_pets}"), poiTags("amenity=animal_training"));
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
    void poiTags_typeWithoutCategory_onlyPoiTag() {
        assertEquals(Map.of(POI_TAG, "{emergency-emergency_ward_entrance}"), poiTags("emergency=emergency_ward_entrance", "building=yes"));
    }

    @Test
    void poiTags_typeWithProjectCategoryOnly_categoryFromRules() {
        assertEquals(Map.of(POI_TAG, "{amenity-school}", POI_CATEGORY_TAG, "{category_school}"), poiTags("amenity=school", "building=yes"));
    }

    @Test
    void poiTags_replacedTag_classifiedAsReplacement() {
        assertEquals(Map.of(POI_TAG, "{amenity-ice_cream}", POI_CATEGORY_TAG, "{category_eat}"), poiTags("shop=ice_cream"));
        assertEquals(Map.of(POI_TAG, "{amenity-parcel_locker}", POI_CATEGORY_TAG, "{category_pickup,category_post}"),
                poiTags("amenity=vending_machine", "vending=parcel_pickup"));
        assertTrue(poiTags("amenity=vending_machine", "vending=drinks").isEmpty());
    }

    @Test
    void poiTags_subwayEntranceIncludedByProjectRule() {
        assertEquals(Map.of(POI_TAG, "{railway-subway_entrance}", POI_CATEGORY_TAG, "{category_metro,category_transport}"),
                poiTags("railway=subway_entrance"));
    }

    @Test
    void poiTags_typesIncludedByProjectRules_projectCategories() {
        assertEquals(Map.of(POI_TAG, "{man_made-bridge}", POI_CATEGORY_TAG, "{category_landmark}"), poiTags("man_made=bridge"));
        assertEquals(Map.of(POI_TAG, "{place-square}", POI_CATEGORY_TAG, "{category_square}"), poiTags("place=square"));
        assertEquals(Map.of(POI_TAG, "{landuse-vineyard}", POI_CATEGORY_TAG, "{category_orchard}"), poiTags("landuse=vineyard"));
        assertEquals(Map.of(POI_TAG, "{landuse-orchard}", POI_CATEGORY_TAG, "{category_orchard}"), poiTags("landuse=orchard"));
    }

    @Test
    void poiTags_divingObjects_diveCategory() {
        Map<String, String> site = Map.of(POI_TAG, "{sport-scuba_diving}", POI_CATEGORY_TAG, "{category_diving,category_sport}");
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
        assertEquals(Map.of(POI_TAG, "{leisure-swimming_area,sport-scuba_diving}", POI_CATEGORY_TAG, "{category_diving,category_sport,category_swimming_area}"),
                poiTags("sport=scuba_diving", "leisure=swimming_area"));
        assertEquals(Map.of(POI_TAG, "{sport-snorkeling}", POI_CATEGORY_TAG, "{category_dive_site}"),
                poiTags("scuba_diving:type:snorkeling=yes"));
        assertEquals(Map.of(POI_TAG, "{natural-reef}", POI_CATEGORY_TAG, "{category_dive_site}"), poiTags("natural=reef"));
        assertEquals(Map.of(POI_TAG, "{sport-freediving}", POI_CATEGORY_TAG, "{category_diving}"), poiTags("sport=freediving"));
    }

    private static OrganicMapsPoiClassifier classifierOf(String mapping, String categories, String rules) throws IOException {
        return new OrganicMapsPoiClassifier(TagReplacer.parse(reader("")), MapcssMapping.parseRules(reader(mapping)),
                Categories.parse(reader(categories)), TypeNames.EMPTY, PoiRules.parse(reader(rules)));
    }

    private static BufferedReader reader(String text) {
        return new BufferedReader(new StringReader(text));
    }

    @Test
    void poiTypes_excludedTypeRemovesTypesOfItsGroup() throws IOException {
        OrganicMapsPoiClassifier parking = classifierOf("amenity|parking;26;\n"
                + "amenity|parking|fee;[amenity=parking][fee];;name;int_name;125;\n"
                + "amenity|parking|private;[amenity=parking][access=private];;name;int_name;145;\n", "", "+amenity-parking\n-amenity-parking-private\n");
        assertEquals(List.of("amenity-parking-fee"), parking.poiTypes(tags("amenity=parking", "fee=yes")));
        assertEquals(List.of(), parking.poiTypes(tags("amenity=parking", "fee=yes", "access=private")));
        assertEquals(List.of(), parking.poiTypes(tags("amenity=parking", "access=private")));
    }

    @Test
    void poiTypes_excludedTypeKeepsOtherGroups() throws IOException {
        OrganicMapsPoiClassifier attractions = classifierOf("attraction|animal;1;\nattraction|carousel;2;\n", "", "+attraction\n-attraction-animal\n");
        assertEquals(List.of("attraction-carousel"), attractions.poiTypes(tags("attraction=animal;carousel")));
        assertEquals(List.of(), attractions.poiTypes(tags("attraction=animal")));
    }

    @Test
    void poiTypes_excludedTypeKeepsTypesIncludedBelowItsRule() throws IOException {
        OrganicMapsPoiClassifier information = classifierOf("tourism|information;1;\n"
                + "tourism|information|board;[tourism=information][information=board];;;;2;\n"
                + "tourism|information|office;[tourism=information][information=office];;;;3;\n", "", "-tourism-information\n+tourism-information-office\n");
        assertEquals(List.of("tourism-information-office"), information.poiTypes(tags("tourism=information", "information=board;office")));
        assertEquals(List.of(), information.poiTypes(tags("tourism=information", "information=board")));
    }

    @Test
    void poiTypes_typeWithoutCategoriesEntryKeepsItsGroup() throws IOException {
        OrganicMapsPoiClassifier shops = classifierOf("shop|a;1;\nshop|a|b;[shop=a][b];;;;2;\nshop|a|c;[shop=a][c];;;;3;\n", "shop-a-c\nen:C\n", "");
        assertEquals(List.of("shop-a-c"), shops.poiTypes(tags("shop=a", "b=yes", "c=yes")));
    }

    @Test
    void poiTypes_exclusionByTagIsStrongerThanInclusion() throws IOException {
        OrganicMapsPoiClassifier sport = classifierOf("leisure|pitch;1;\nsport|soccer;2;\namenity|school;3;\n", "",
                "+leisure-pitch\n+sport-soccer\n+amenity-school\n-leisure access=private;no\n-sport access=private;no\n");
        assertEquals(List.of("leisure-pitch", "sport-soccer"), sport.poiTypes(tags("leisure=pitch", "sport=soccer")));
        assertEquals(List.of("leisure-pitch", "sport-soccer"), sport.poiTypes(tags("leisure=pitch", "sport=soccer", "access=yes")));
        assertEquals(List.of(), sport.poiTypes(tags("leisure=pitch", "sport=soccer", "access=private")));
        assertEquals(List.of(), sport.poiTypes(tags("leisure=pitch", "sport=soccer", "access=no")));
        assertEquals(List.of("amenity-school"), sport.poiTypes(tags("amenity=school", "sport=soccer", "access=private")));
    }

    @Test
    void poiTypes_exclusionRulesOfOneTypeAreAlternatives() throws IOException {
        OrganicMapsPoiClassifier snorkeling = classifierOf("sport|snorkeling;[sport=snorkeling];;;;1;\n", "",
                "+sport-snorkeling\n-sport-snorkeling tourism=hotel;hostel\n-sport-snorkeling office=*\n");
        assertEquals(List.of("sport-snorkeling"), snorkeling.poiTypes(tags("sport=snorkeling")));
        assertEquals(List.of("sport-snorkeling"), snorkeling.poiTypes(tags("sport=snorkeling", "tourism=attraction")));
        assertEquals(List.of(), snorkeling.poiTypes(tags("sport=snorkeling", "tourism=hostel")));
        assertEquals(List.of(), snorkeling.poiTypes(tags("sport=snorkeling", "office=company")));
    }

    @Test
    void poiTypes_typeExcludedByTagLeavesTheShorterType() throws IOException {
        OrganicMapsPoiClassifier diving = classifierOf("sport|scuba_diving;1;\nsport|scuba_diving|divespot;[sport=scuba_diving][scuba_diving:type];;;;2;\n", "",
                "+sport-scuba_diving\n-sport-scuba_diving-divespot tourism=hotel\n");
        assertEquals(List.of("sport-scuba_diving-divespot"), diving.poiTypes(tags("sport=scuba_diving", "scuba_diving:type=reef")));
        assertEquals(List.of("sport-scuba_diving"), diving.poiTypes(tags("sport=scuba_diving", "scuba_diving:type=reef", "tourism=hotel")));
    }

    @Test
    void poiTags_privateObjectsAreNotPoi() {
        assertEquals(Map.of(POI_TAG, "{amenity-parking-multi-storey}", POI_CATEGORY_TAG, "{category_parking}"), poiTags("amenity=parking", "parking=multi-storey"));
        assertTrue(poiTags("amenity=parking", "parking=multi-storey", "access=private").isEmpty());
        assertTrue(poiTags("amenity=parking", "parking=multi-storey", "access=no").isEmpty());
        assertTrue(poiTags("amenity=parking", "fee=yes", "access=private").isEmpty());
        assertTrue(poiTags("leisure=pitch", "sport=soccer", "access=private").isEmpty());
        assertTrue(poiTags("leisure=playground", "access=no").isEmpty());
        assertTrue(poiTags("leisure=garden", "access=private").isEmpty());
        assertTrue(poiTags("man_made=pier", "access=private").isEmpty());
        assertTrue(poiTags("amenity=charging_station", "access=private").isEmpty());
        assertTrue(poiTags("amenity=water_point", "access=private").isEmpty());
        assertTrue(poiTags("amenity=drinking_water", "access=no").isEmpty());
        assertTrue(poiTags("man_made=water_tap", "access=private").isEmpty());
        assertTrue(poiTags("man_made=water_well", "access=private").isEmpty());
        assertEquals(Map.of(POI_TAG, "{leisure-garden}", POI_CATEGORY_TAG, "{category_park}"), poiTags("leisure=garden"));
        assertEquals(Map.of(POI_TAG, "{leisure-pitch,sport-soccer}", POI_CATEGORY_TAG, "{category_pitch,category_sport}"), poiTags("leisure=pitch", "sport=soccer", "access=customers"));
        assertEquals(Map.of(POI_TAG, "{amenity-school}", POI_CATEGORY_TAG, "{category_school}"), poiTags("amenity=school", "sport=soccer", "access=private"));
        assertEquals(Map.of(POI_TAG, "{amenity-cafe}", POI_CATEGORY_TAG, "{category_eat}"), poiTags("amenity=cafe", "access=private"));
    }

    @Test
    void poiTags_excludedSubtypeRemovesItsGroup() {
        assertEquals(Map.of(POI_TAG, "{natural-water-lake}", POI_CATEGORY_TAG, "{category_lake}"), poiTags("natural=water", "water=lake", "name=Lac"));
        assertTrue(poiTags("natural=water", "water=lake", "name=Lac", "intermittent=yes").isEmpty());
        assertEquals(Map.of(POI_TAG, "{amenity-cafe}", POI_CATEGORY_TAG, "{category_eat}"), poiTags("landuse=industrial", "industrial=warehouse", "amenity=cafe", "name=Depot"));
        assertEquals(Map.of(POI_TAG, "{amenity-cafe}", POI_CATEGORY_TAG, "{category_eat}"), poiTags("landuse=industrial", "industrial=depot", "amenity=cafe", "name=Depot"));
        assertEquals(work("landuse-industrial-warehouse", "category_warehouse"), poiTags("landuse=industrial", "industrial=warehouse", "name=Depot"));
    }

    @Test
    void poiTags_informationAndAttractionsWithSeveralValues() {
        assertEquals(Map.of(POI_TAG, "{tourism-information-office}", POI_CATEGORY_TAG, "{category_tourism}"), poiTags("tourism=information", "information=board;office"));
        assertEquals(Map.of(POI_TAG, "{attraction-carousel}", POI_CATEGORY_TAG, "{category_amusement}"), poiTags("attraction=animal;carousel"));
    }

    @Test
    void poiTags_businessesAreNotDiveSites() {
        Map<String, String> centre = Map.of(POI_TAG, "{amenity-dive_centre,sport-scuba_diving}", POI_CATEGORY_TAG, "{category_diving,category_sport}");
        assertEquals(centre, poiTags("amenity=dive_centre", "sport=scuba_diving;snorkeling"));
        assertEquals(centre, poiTags("amenity=dive_centre", "sport=scuba_diving", "scuba_diving:entry:boat=yes"));
        assertEquals(centre, poiTags("amenity=dive_centre", "sport=scuba_diving", "scuba_diving:type:cave=yes"));
        assertEquals(centre, poiTags("amenity=dive_centre", "sport=scuba_diving", "sport=snorkling"));
        assertEquals(Map.of(POI_TAG, "{shop-scuba_diving}", POI_CATEGORY_TAG, "{category_diving}"), poiTags("shop=scuba_diving", "sport=snorkeling"));
        assertEquals(Map.of(POI_TAG, "{leisure-pitch}", POI_CATEGORY_TAG, "{category_pitch}"), poiTags("leisure=pitch", "scuba_diving:divespot=yes"));
        assertEquals(Map.of(POI_TAG, "{tourism-hotel}", POI_CATEGORY_TAG, "{category_hotel}"), poiTags("tourism=hotel", "scuba_diving:type:snorkeling=yes"));
        assertEquals(Map.of(POI_TAG, "{sport-scuba_diving,tourism-hotel}", POI_CATEGORY_TAG, "{category_diving,category_hotel,category_sport}"),
                poiTags("tourism=hotel", "sport=scuba_diving", "scuba_diving:entry:boat=yes"));
        assertEquals(work("office-company", "category_office"), poiTags("office=company", "sport=snorkeling"));
        assertEquals(Map.of(POI_TAG, "{sport-snorkeling}", POI_CATEGORY_TAG, "{category_dive_site}"), poiTags("sport=snorkeling", "tourism=attraction"));
    }

    @Test
    void poiTags_rentalShopAndTravelAgency() {
        assertEquals(Map.of(POI_TAG, "{shop-rental}", POI_CATEGORY_TAG, "{category_services}"), poiTags("shop=rental"));
        assertEquals(work("shop-travel_agency", "category_office_services"), poiTags("shop=travel_agency"));
    }

    @Test
    void poiTags_negativeFactorsAndSmallObjectsAreNotPoi() {
        assertTrue(poiTags("amenity=prison").isEmpty());
        assertTrue(poiTags("shop=funeral_directors").isEmpty());
        assertTrue(poiTags("amenity=waste_transfer_station").isEmpty());
        assertTrue(poiTags("leisure=garden", "garden:type=residential").isEmpty());
        assertTrue(poiTags("amenity=parking_space", "parking_space=disabled").isEmpty());
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
        assertEquals(Map.of(POI_TAG, "{man_made-pier}", POI_CATEGORY_TAG, "{category_pier}"), poiTags("man_made=pier"));
        assertEquals(Map.of(POI_TAG, "{place-island}", POI_CATEGORY_TAG, "{category_coast}"), poiTags("place=island"));
        assertEquals(Map.of(POI_TAG, "{place-islet}", POI_CATEGORY_TAG, "{category_coast}"), poiTags("place=islet"));
        assertEquals(Map.of(POI_TAG, "{natural-bay}", POI_CATEGORY_TAG, "{category_coast}"), poiTags("natural=bay"));
        assertEquals(Map.of(POI_TAG, "{natural-cape}", POI_CATEGORY_TAG, "{category_coast}"), poiTags("natural=cape"));
        assertEquals(Map.of(), poiTags("natural=cliff"));
        assertEquals(Map.of(POI_TAG, "{natural-cliff-notable}", POI_CATEGORY_TAG, "{category_mountain}"), poiTags("natural=cliff", "name=Aiguille"));
        assertEquals(Map.of(POI_TAG, "{natural-cliff-notable}", POI_CATEGORY_TAG, "{category_mountain}"), poiTags("natural=cliff", "climbing=crag"));
        assertEquals(Map.of(POI_TAG, "{natural-cliff-notable}", POI_CATEGORY_TAG, "{category_mountain}"), poiTags("natural=cliff", "sport=climbing"));
        assertEquals(Map.of(POI_TAG, "{leisure-ice_rink}", POI_CATEGORY_TAG, "{category_sport}"), poiTags("leisure=ice_rink"));
        assertEquals(Map.of(POI_TAG, "{boundary-national_park}", POI_CATEGORY_TAG, "{category_protected_area}"), poiTags("boundary=national_park"));
        assertEquals(Map.of(POI_TAG, "{leisure-swimming_area}", POI_CATEGORY_TAG, "{category_swimming_area}"), poiTags("leisure=swimming_area"));
    }

    @Test
    void poiTags_projectLeisureTypes_poiByProjectMapping() {
        assertEquals(Map.of(POI_TAG, "{leisure-fishing}", POI_CATEGORY_TAG, "{category_water_activities}"), poiTags("leisure=fishing"));
        assertEquals(work("amenity-coworking_space", "category_coworking"), poiTags("amenity=coworking_space"));
        assertEquals(work("amenity-coworking_space,office-coworking", "category_coworking"), poiTags("office=co_working"));
        assertEquals(Map.of(POI_TAG, "{leisure-horse_riding}", POI_CATEGORY_TAG, "{category_sport}"), poiTags("leisure=horse_riding"));
        assertEquals(Map.of(POI_TAG, "{leisure-spa}", POI_CATEGORY_TAG, "{category_wellness}"), poiTags("leisure=spa"));
        assertEquals(Map.of(POI_TAG, "{leisure-spa}", POI_CATEGORY_TAG, "{category_wellness}"), poiTags("amenity=spa"));
        assertEquals(Map.of(POI_TAG, "{leisure-spa}", POI_CATEGORY_TAG, "{category_wellness}"), poiTags("tourism=spa_resort"));
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
        assertEquals(Map.of(POI_TAG, "{man_made-tower}", POI_CATEGORY_TAG, "{category_landmark}"), poiTags("man_made=tower", "tower:type=water;observation"));
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
        assertEquals(Map.of(POI_TAG, "{man_made-tower}", POI_CATEGORY_TAG, "{category_landmark}"), poiTags("man_made=tower", "tower:type=observation"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}", POI_CATEGORY_TAG, "{category_landmark}"), poiTags("man_made=tower", "tower:type=bell_tower"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}", POI_CATEGORY_TAG, "{category_landmark}"), poiTags("man_made=tower", "tower:type=pagoda"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}", POI_CATEGORY_TAG, "{category_landmark}"), poiTags("man_made=tower", "tower:type=stupa"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}", POI_CATEGORY_TAG, "{category_landmark}"), poiTags("man_made=tower", "tower:type=chedi"));
        assertEquals(Map.of(POI_TAG, "{man_made-tower}", POI_CATEGORY_TAG, "{category_landmark}"), poiTags("man_made=tower", "tower:type=lighting; observation"));
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
        assertEquals(Map.of(POI_TAG, "{amenity-cafe,man_made-tower}", POI_CATEGORY_TAG, "{category_eat,category_landmark}"),
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

        assertEquals(List.of("category_school"), catalog.get("amenity-school").getCategories());
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
