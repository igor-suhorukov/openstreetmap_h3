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
    void poiTypes_includedByProjectRules() {
        assertEquals(List.of("man_made-bridge"), classifier.poiTypes(tags("man_made=bridge")));
        assertEquals(List.of("place-square"), classifier.poiTypes(tags("place=square")));
        assertEquals(List.of("landuse-vineyard"), classifier.poiTypes(tags("landuse=vineyard")));
        assertEquals(List.of("landuse-orchard"), classifier.poiTypes(tags("landuse=orchard")));
    }

    @Test
    void poiTypes_lakesRiversGlaciersOnlyWithName() {
        assertEquals(List.of("natural-water-lake"), classifier.poiTypes(tags("natural=water", "water=lake", "name=Lac Blanc")));
        assertEquals(List.of("natural-water"), classifier.poiTypes(tags("natural=water", "name=Lac Blanc")));
        assertEquals(List.of("natural-water-reservoir"), classifier.poiTypes(tags("natural=water", "water=reservoir", "name=Lac")));
        assertEquals(List.of("natural-water-pond"), classifier.poiTypes(tags("natural=water", "water=pond", "name=Etang")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=water", "water=lake")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=water", "water=wastewater", "name=Station")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=water", "water=river", "name=Arve")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=water", "water=stream", "name=Ruisseau")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=water", "water=canal", "name=Canal")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=water", "water=basin", "name=Bassin")));
        assertEquals("{category_lake}", categoryOf("natural=water", "water=lake", "name=Lac Blanc"));
        assertEquals(List.of("waterway-river"), classifier.poiTypes(tags("waterway=river", "name=Arve")));
        assertEquals(List.of("waterway-canal"), classifier.poiTypes(tags("waterway=canal", "name=Canal")));
        assertEquals(List.of(), classifier.poiTypes(tags("waterway=river")));
        assertEquals(List.of(), classifier.poiTypes(tags("waterway=river", "tunnel=yes", "name=Arve")));
        assertEquals(List.of(), classifier.poiTypes(tags("waterway=river", "intermittent=yes", "name=Arve")));
        assertEquals("{category_river}", categoryOf("waterway=river", "name=Arve"));
        assertEquals(List.of("natural-glacier"), classifier.poiTypes(tags("natural=glacier", "name=Mer de Glace")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=glacier")));
    }

    @Test
    void poiTypes_paymentTerminalsAndGuardhousesAreNotPoi() {
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=payment_terminal", "name=Terminal")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=guardhouse", "name=Gate")));
    }

    @Test
    void poiTypes_privateParksAndPoolsAreNotPoi() {
        assertEquals(List.of(), classifier.poiTypes(tags("leisure=park", "access=private")));
        assertEquals(List.of(), classifier.poiTypes(tags("leisure=park", "access=no")));
        assertEquals(List.of("leisure-park"), classifier.poiTypes(tags("leisure=park")));
        assertEquals(List.of("leisure-park-permissive"), classifier.poiTypes(tags("leisure=park", "access=permissive")));
        assertEquals(List.of(), classifier.poiTypes(tags("leisure=swimming_pool", "access=private")));
        assertEquals(List.of(), classifier.poiTypes(tags("leisure=swimming_pool", "access=no")));
        assertEquals(List.of("leisure-swimming_pool"), classifier.poiTypes(tags("leisure=swimming_pool")));
    }

    @Test
    void poiTypes_protectedArea_onlyNamedClasses235() {
        assertEquals(List.of("boundary-protected_area-2"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=2", "name=Park")));
        assertEquals(List.of("boundary-protected_area-5"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=5", "name=Park")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=2")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=1a", "name=Park")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=4", "name=Park")));
    }

    @Test
    void poiTypes_craftsAndGastronomy() {
        assertEquals(List.of("craft-pottery"), classifier.poiTypes(tags("craft=pottery")));
        assertTrue(classifier.poiTypes(tags("shop=pottery")).contains("craft-pottery"));
        assertEquals(List.of("craft-distillery"), classifier.poiTypes(tags("craft=distillery")));
        assertEquals(List.of(), classifier.poiTypes(tags("craft=stonemason", "name=Monuments")));
        assertEquals(List.of("craft-oil_mill"), classifier.poiTypes(tags("craft=oil_mill")));
        assertEquals(List.of("craft-boatbuilder"), classifier.poiTypes(tags("craft=boatbuilder")));
        assertEquals(List.of("craft-atelier"), classifier.poiTypes(tags("craft=atelier", "name=Studio")));
        assertEquals(List.of(), classifier.poiTypes(tags("craft=atelier")));
        assertTrue(categoryOf("craft=winery").contains("category_craft_drink"));
        assertTrue(categoryOf("craft=brewery").contains("category_craft_drink"));
        assertTrue(categoryOf("craft=handicraft").contains("category_crafts"));
        assertTrue(categoryOf("craft=confectionery").contains("category_craft_food"));
        assertTrue(categoryOf("craft=beekeeper").contains("category_craft_food"));
    }

    @Test
    void poiTypes_placesOfWork() {
        assertEquals(List.of("office-company"), classifier.poiTypes(tags("office=company")));
        assertEquals(List.of("office"), classifier.poiTypes(tags("office=it")));
        assertEquals(List.of("office-lawyer"), classifier.poiTypes(tags("office=notary")));
        assertEquals(List.of("office-estate_agent"), classifier.poiTypes(tags("shop=estate_agent")));
        assertEquals(List.of("office-travel_agent", "shop-travel_agency"), classifier.poiTypes(tags("office=travel_agent")));
        assertEquals(List.of("office-government"), classifier.poiTypes(tags("office=administrative")));
        assertEquals(List.of("office-diplomatic"), classifier.poiTypes(tags("amenity=embassy")));
        assertEquals(List.of("amenity-research_institute", "office-research"), classifier.poiTypes(tags("amenity=research_institute", "office=research")));
        assertEquals(List.of("building-office"), classifier.poiTypes(tags("building=office", "name=Tower")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=office")));
        assertEquals(List.of("shop-mall"), classifier.poiTypes(tags("building=office", "shop=mall", "name=Mall")));
        assertEquals(List.of("landuse-commercial"), classifier.poiTypes(tags("landuse=commercial", "name=Business Park")));
        assertEquals(List.of("tourism-hotel"), classifier.poiTypes(tags("landuse=commercial", "tourism=hotel", "name=Hotel")));
        assertEquals(List.of("man_made-works"), classifier.poiTypes(tags("man_made=works", "name=Plant")));
        assertEquals(List.of("landuse-industrial-depot"), classifier.poiTypes(tags("landuse=industrial", "industrial=depot", "name=Depot")));
        assertEquals(List.of("landuse-industrial-warehouse"), classifier.poiTypes(tags("landuse=industrial", "industrial=storage", "name=Stock")));
        assertEquals("{category_office,category_work}", categoryOf("office=yes"));
        assertEquals("{category_office_services,category_work}", categoryOf("office=insurance"));
        assertEquals("{category_office_services,category_work}", categoryOf("office=travel_agent"));
        assertEquals("{category_government,category_work}", categoryOf("amenity=townhall"));
        assertEquals("{category_ngo,category_work}", categoryOf("office=charity"));
        assertEquals("{category_coworking,category_work}", categoryOf("amenity=coworking_space"));
        assertEquals("{category_business_centre,category_work}", categoryOf("building=office", "name=Tower"));
        assertEquals("{category_industrial,category_work}", categoryOf("landuse=industrial", "name=Plant"));
        assertEquals("{category_warehouse,category_work}", categoryOf("landuse=industrial", "industrial=logistics", "name=Hub"));
        assertEquals("{category_depot,category_work}", categoryOf("landuse=industrial", "industrial=depot", "name=Depot"));
        assertEquals("{category_research,category_work}", categoryOf("amenity=research_institute"));
    }

    @Test
    void poiTypes_universitiesAndColleges() {
        assertEquals("{category_college_university}", categoryOf("amenity=university"));
        assertEquals("{category_college_university}", categoryOf("amenity=college"));
        assertEquals(List.of("building-university"), classifier.poiTypes(tags("building=university", "name=Main Hall")));
        assertEquals(List.of("building-college"), classifier.poiTypes(tags("building=college", "name=Workshop")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=university")));
        assertEquals(List.of("amenity-university"), classifier.poiTypes(tags("building=university", "amenity=university", "name=Campus")));
        assertEquals(List.of("amenity-research_institute"), classifier.poiTypes(tags("building=university", "amenity=research_institute", "name=Lab")));
        assertEquals(List.of("building-university", "office-educational_institution"),
                classifier.poiTypes(tags("building=university", "office=educational_institution", "name=Office")));
        assertEquals("{category_college_university}", categoryOf("building=college", "name=Workshop"));
    }

    @Test
    void poiTypes_geotourismTypes() {
        assertEquals(List.of("natural-rock"), classifier.poiTypes(tags("natural=stone", "name=Pierre")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=stone")));
        assertEquals(List.of("natural-arch"), classifier.poiTypes(tags("natural=arch")));
        assertEquals(List.of("natural-sinkhole"), classifier.poiTypes(tags("natural=sinkhole", "name=Aven")));
        assertTrue(categoryOf("natural=cave_entrance").contains("category_geotourism"));
        assertTrue(categoryOf("natural=arch").contains("category_geotourism"));
    }

    @Test
    void poiTypes_lavenderFieldIsPoiPrivateAndOtherFarmlandAreNot() {
        assertEquals(List.of("landuse-farmland-lavender"), classifier.poiTypes(tags("landuse=farmland", "crop=lavender")));
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=farmland", "crop=lavender", "access=private")));
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=farmland", "crop=wheat")));
    }

    @Test
    void poiTypes_swimmingAreaIsPoiCemeteryIsNot() {
        assertEquals(List.of("leisure-swimming_area"), classifier.poiTypes(tags("leisure=swimming_area")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=grave_yard", "name=Old Cemetery")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=grave_yard", "religion=christian")));
    }

    @Test
    void poiTypes_skiSheltersAndAirports() {
        assertEquals(List.of("amenity-ski_rental"), classifier.poiTypes(tags("amenity=ski_rental")));
        assertEquals(List.of("amenity-ski_school"), classifier.poiTypes(tags("amenity=ski_school")));
        assertEquals(List.of("shop-ski"), classifier.poiTypes(tags("shop=ski")));
        assertEquals("{category_ski}", categoryOf("amenity=ski_school"));
        assertEquals("{category_shopping,category_ski}", categoryOf("shop=ski"));
        assertEquals(List.of("amenity-shelter-basic_hut"), classifier.poiTypes(tags("amenity=shelter", "shelter_type=basic_hut")));
        assertEquals(List.of("amenity-shelter-lean_to"), classifier.poiTypes(tags("amenity=shelter", "shelter_type=lean_to")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=shelter")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=shelter", "shelter_type=public_transport")));
        assertEquals(List.of("aeroway-aerodrome"), classifier.poiTypes(tags("aeroway=aerodrome", "name=Megeve")));
        assertEquals(List.of("aeroway-aerodrome-international"), classifier.poiTypes(tags("aeroway=aerodrome", "aerodrome=international", "name=Geneve")));
        assertEquals(List.of(), classifier.poiTypes(tags("aeroway=aerodrome")));
        assertEquals(List.of(), classifier.poiTypes(tags("aeroway=aerodrome", "aerodrome=mountain airfield", "name=Altisurface")));
        assertEquals(List.of(), classifier.poiTypes(tags("aeroway=aerodrome", "aerodrome:type=military", "name=Base")));
        assertEquals(List.of(), classifier.poiTypes(tags("aeroway=aerodrome", "aerodrome:type=private", "name=Strip")));
        assertEquals("{category_transport}", categoryOf("aeroway=aerodrome", "name=Megeve"));
    }

    @Test
    void poiTags_divingAndAerialwayStations_ownCategories() {
        assertEquals(List.of("sport-scuba_diving"), classifier.poiTypes(tags("sport=scuba_diving")));
        assertEquals(List.of("sport-scuba_diving-divespot"), classifier.poiTypes(tags("scuba_diving:divespot=yes")));
        assertEquals(List.of("amenity-dive_centre"), classifier.poiTypes(tags("amenity=dive_centre")));
        assertEquals(List.of("shop-scuba_diving"), classifier.poiTypes(tags("shop=scuba_diving")));
        assertEquals(List.of("aerialway-station"), classifier.poiTypes(tags("aerialway=station")));
        assertEquals(List.of(), classifier.poiTypes(tags("aerialway=gondola")));

        assertEquals("{category_diving}", categoryOf("sport=scuba_diving"));
        assertEquals("{category_dive_site,category_diving}", categoryOf("scuba_diving:divespot=yes"));
        assertEquals("{category_diving}", categoryOf("amenity=dive_centre"));
        assertEquals("{category_diving}", categoryOf("shop=scuba_diving"));
        assertEquals("{category_aerialway}", categoryOf("aerialway=station"));
    }

    @Test
    void poiTags_waterBeachesAndPeaks_ownCategories() {
        assertEquals("{category_drinking_water,category_water}", categoryOf("amenity=drinking_water"));
        assertEquals("{category_drinking_water,category_water}", categoryOf("man_made=water_tap"));
        assertEquals("{category_drinking_water,category_water}", categoryOf("man_made=water_well"));
        assertEquals("{category_drinking_water,category_rv,category_water}", categoryOf("amenity=water_point"));
        assertEquals("{category_springs,category_water}", categoryOf("natural=spring"));
        assertEquals("{category_springs,category_water}", categoryOf("natural=hot_spring"));
        assertEquals("{category_springs}", categoryOf("natural=geyser"));
        assertEquals("{category_beach}", categoryOf("natural=beach"));
        assertEquals("{category_beach}", categoryOf("leisure=beach_resort"));
        assertEquals("{category_peak}", categoryOf("natural=peak"));
        assertEquals("{category_volcano}", categoryOf("natural=volcano"));
    }

    @Test
    void poiTags_diveSitesFromRealTagging() {
        assertEquals(List.of("sport-scuba_diving-divespot", "tourism-attraction"),
                classifier.poiTypes(tags("sport=scuba_diving", "scuba_diving:entry:boat=yes", "tourism=attraction")));
        assertEquals(List.of("natural-reef", "sport-scuba_diving-divespot", "tourism-attraction"),
                classifier.poiTypes(tags("natural=reef", "sport=scuba_diving", "tourism=attraction")));
        assertEquals(List.of("natural-cave_entrance", "sport-scuba_diving-divespot"),
                classifier.poiTypes(tags("natural=cave_entrance", "scuba_diving:type:cave=yes", "sport=scuba_diving")));
        assertEquals(List.of("leisure-swimming_pool", "sport-scuba_diving"),
                classifier.poiTypes(tags("leisure=swimming_pool", "scuba_diving:type:pool=yes", "sport=scuba_diving")));
        assertEquals(List.of("sport-scuba_diving-divespot"),
                classifier.poiTypes(tags("sport=scuba_diving", "scuba_diving:entry=boat", "scuba_diving:type=reef")));
        assertEquals(List.of("sport-scuba_diving-divespot"),
                classifier.poiTypes(tags("sport=scuba_diving", "scuba_diving:entry=shore")));
        assertEquals(List.of("sport-scuba_diving-divespot"),
                classifier.poiTypes(tags("sport=scuba_diving", "scuba_diving:type=wreck")));
        assertEquals(List.of("sport-scuba_diving-divespot"),
                classifier.poiTypes(tags("sport=scuba_diving", "scuba_diving:type:wall=yes", "scuba_diving:type:drift=yes")));
        assertEquals(List.of("sport-scuba_diving"),
                classifier.poiTypes(tags("sport=scuba_diving", "scuba_diving:entry=no")));
        assertEquals(List.of("sport-scuba_diving"),
                classifier.poiTypes(tags("sport=scuba_diving", "scuba_diving:type:training=yes")));
        assertEquals(List.of("amenity-dive_centre", "sport-scuba_diving"),
                classifier.poiTypes(tags("amenity=dive_centre", "sport=scuba_diving", "scuba_diving:entry=boat", "scuba_diving:type=reef")));
        assertEquals(List.of("shop-scuba_diving", "sport-scuba_diving"),
                classifier.poiTypes(tags("shop=scuba_diving", "sport=scuba_diving", "scuba_diving:type=reef")));
        assertEquals(List.of("leisure-swimming_pool", "sport-scuba_diving"),
                classifier.poiTypes(tags("leisure=swimming_pool", "sport=scuba_diving", "scuba_diving:type=pool")));
        assertEquals(List.of("historic-wreck"), classifier.poiTypes(tags("seamark:type=wreck")));
        assertEquals("{category_dive_site}", categoryOf("natural=reef"));
        assertEquals("{category_dive_site}", categoryOf("historic=wreck"));
    }

    @Test
    void poiTypes_healthcareTagOnly_sameTypesAsAmenity() {
        assertEquals(List.of("amenity-clinic"), classifier.poiTypes(tags("healthcare=clinic")));
        assertEquals(List.of("amenity-hospital"), classifier.poiTypes(tags("healthcare=hospital")));
        assertEquals(List.of("amenity-doctors"), classifier.poiTypes(tags("healthcare=doctor")));
        assertEquals(List.of("amenity-dentist"), classifier.poiTypes(tags("healthcare=dentist")));
        assertEquals(List.of("amenity-pharmacy"), classifier.poiTypes(tags("healthcare=pharmacy")));
        assertEquals(List.of("healthcare-other"), classifier.poiTypes(tags("healthcare=yes")));
        assertEquals("{category_sanatorium}", categoryOf("amenity=sanatorium"));
        assertEquals("{category_sanatorium}", categoryOf("healthcare=rehabilitation"));
        assertEquals(List.of("healthcare-sanatorium", "leisure-resort"), classifier.poiTypes(tags("leisure=resort", "resort=sanatorium")));
        assertEquals(List.of("amenity-doctors", "healthcare-trauma"), classifier.poiTypes(tags("amenity=doctors", "healthcare=doctor", "healthcare:speciality=trauma")));
        assertEquals("{category_hospital,category_trauma}", categoryOf("amenity=clinic", "healthcare:speciality=neurology;trauma"));
        assertEquals("{category_trauma}", categoryOf("healthcare:speciality=trauma"));
        assertEquals(List.of("emergency-emergency_ward_entrance"), classifier.poiTypes(tags("emergency=emergency_ward_entrance")));
    }

    @Test
    void poiTypes_cultureTags() {
        assertEquals(List.of("historic-yes"), classifier.poiTypes(tags("historic=yes")));
        assertEquals("{category_tourism}", categoryOf("historic=monastery"));
        assertEquals("{category_tourism}", categoryOf("historic=heritage"));
        assertEquals(List.of("historic-vehicle"), classifier.poiTypes(tags("historic=railway_car")));
        assertEquals(List.of("historic-military"), classifier.poiTypes(tags("historic=bunker")));
        assertEquals(List.of("historic-memorial-plaque"), classifier.poiTypes(tags("historic=epigraph")));
        assertEquals(List.of("amenity-events_venue"), classifier.poiTypes(tags("amenity=music_venue")));
        assertEquals("{category_tourism}", categoryOf("amenity=planetarium"));
        assertEquals("{category_tourism}", categoryOf("man_made=observatory"));
        assertEquals(List.of(), classifier.poiTypes(tags("historic=suburb")));
    }

    @Test
    void poiTags_attractions_amusementCategoryWithoutAnimals() {
        assertEquals(List.of("attraction-water_slide"), classifier.poiTypes(tags("attraction=water_slide")));
        assertTrue(categoryOf("attraction=roller_coaster").contains("category_amusement"));
        assertTrue(categoryOf("attraction=pirate_ship").contains("category_amusement"));
        assertEquals(List.of(), classifier.poiTypes(tags("attraction=animal")));
    }

    @Test
    void poiTypes_sportTagOnly_everyKindHasAType() {
        for (String water : List.of("surfing", "kitesurfing", "windsurfing", "canoe", "rowing", "sailing", "wakeboarding")) {
            assertEquals(List.of("sport-water_sports"), classifier.poiTypes(tags("sport=" + water)), water);
        }
        assertEquals(List.of("sport-air_sports"), classifier.poiTypes(tags("sport=free_flying")));
        assertEquals(List.of("sport-adventure"), classifier.poiTypes(tags("sport=climbing_adventure")));
        assertEquals(List.of("sport-martial_arts"), classifier.poiTypes(tags("sport=muaythai")));
        assertEquals(List.of("sport-winter_sports"), classifier.poiTypes(tags("sport=ice_skating")));
        assertEquals(List.of("sport-indoor_games"), classifier.poiTypes(tags("sport=billiards")));
        assertEquals(List.of("sport-soccer"), classifier.poiTypes(tags("sport=football")));
        assertEquals(List.of("sport-ice_hockey"), classifier.poiTypes(tags("sport=ice_hockey")));
        assertEquals(List.of("sport-multi"), classifier.poiTypes(tags("sport=multi")));
        assertEquals(List.of("sport-club"), classifier.poiTypes(tags("club=sport")));
    }

    @Test
    void poiTags_animalPlaces_petsCategory() {
        assertEquals("{category_pets}", categoryOf("amenity=veterinary"));
        assertEquals("{category_pets}", categoryOf("leisure=dog_park"));
        assertEquals("{category_pets}", categoryOf("shop=pet_grooming"));
        assertEquals("{category_pets}", categoryOf("amenity=animal_training"));
    }

    @Test
    void poiTags_parkingBankAndAtm_separateCategories() {
        assertEquals("{category_parking}", categoryOf("amenity=parking"));
        assertEquals("{category_atm}", categoryOf("amenity=atm"));
        assertEquals("{category_bank}", categoryOf("amenity=bank"));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=parking", "access=private")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=parking", "access=no")));
    }

    @Test
    void poiTags_metroAndDojo() {
        assertEquals("{category_metro,category_transport}", categoryOf("railway=subway_entrance"));
        assertEquals("{category_metro,category_transport}", categoryOf2("railway=station", "station=subway"));
        assertEquals(List.of("amenity-dojo"), classifier.poiTypes(tags("amenity=dojo")));
        assertEquals(List.of("railway-halt-mcc"), classifier.poiTypes(tags("railway=halt", "network=МЦК")));
        assertEquals("{category_metro,category_transport}", categoryOf2("railway=halt", "network=МЦК"));
        assertEquals(List.of("railway-halt"), classifier.poiTypes(tags("railway=halt", "network=МЦД")));
        assertEquals("{category_transport}", categoryOf2("railway=halt", "network=МЦД"));
        assertEquals("{category_transport}", categoryOf("railway=station"));
    }

    @Test
    void poiTypes_tourismInformation_onlyOfficesArePoi() {
        assertEquals(List.of(), classifier.poiTypes(tags("tourism=information")));
        assertEquals(List.of(), classifier.poiTypes(tags("tourism=information", "information=guidepost")));
        assertEquals(List.of(), classifier.poiTypes(tags("tourism=information", "information=board")));
        assertEquals(List.of(), classifier.poiTypes(tags("tourism=information", "information=map")));
        assertEquals(List.of(), classifier.poiTypes(tags("tourism=information", "information=route_marker")));
        assertEquals("{category_tourism}", categoryOf2("tourism=information", "information=office"));
        assertEquals(List.of("tourism-information-visitor_centre"), classifier.poiTypes(tags("tourism=information", "information=visitor_centre")));
    }

    private static String categoryOf2(String first, String second) {
        return classifier.poiTags(tags(first, second)).stream()
                .filter(poiTag -> poiTag.getKey().equals(OrganicMapsPoiClassifier.POI_CATEGORY_TAG))
                .findFirst().orElseThrow().getValue();
    }

    @Test
    void poiTypes_coastNatureAndLeisureTypes() {
        assertEquals(List.of("man_made-pier"), classifier.poiTypes(tags("man_made=pier")));
        assertEquals(List.of("place-island"), classifier.poiTypes(tags("place=island")));
        assertEquals(List.of("place-islet"), classifier.poiTypes(tags("place=islet")));
        assertEquals(List.of("natural-bay"), classifier.poiTypes(tags("natural=bay")));
        assertEquals(List.of("natural-cape"), classifier.poiTypes(tags("natural=cape")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=cliff")));
        assertEquals(List.of("natural-cliff-notable"), classifier.poiTypes(tags("natural=cliff", "name=Aiguille")));
        assertEquals(List.of("natural-cliff-notable"), classifier.poiTypes(tags("natural=cliff", "climbing=crag")));
        assertEquals(List.of("natural-cliff-notable", "sport-climbing"), classifier.poiTypes(tags("natural=cliff", "sport=climbing")));
        assertEquals(List.of("leisure-ice_rink"), classifier.poiTypes(tags("leisure=ice_rink")));
        assertEquals(List.of("boundary-national_park"), classifier.poiTypes(tags("boundary=national_park")));
        assertEquals(List.of("leisure-fishing"), classifier.poiTypes(tags("leisure=fishing")));
        assertEquals(List.of("amenity-coworking_space"), classifier.poiTypes(tags("amenity=coworking_space")));
        assertEquals(List.of("amenity-coworking_space", "office-coworking"), classifier.poiTypes(tags("office=coworking")));
        assertEquals(List.of("leisure-horse_riding"), classifier.poiTypes(tags("leisure=horse_riding")));
        assertEquals(List.of("leisure-spa"), classifier.poiTypes(tags("tourism=spa_resort")));
        assertEquals(List.of(), classifier.poiTypes(tags("place=city")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area")));
    }

    @Test
    void poiTypes_severalValuesOfOneTag_everyValueMatched() {
        assertEquals(List.of("tourism-gallery", "tourism-museum"), classifier.poiTypes(tags("tourism=museum;gallery")));
        assertEquals(List.of("natural-peak", "natural-volcano"), classifier.poiTypes(tags("natural=peak;volcano")));
        assertEquals(List.of("shop-alcohol", "shop-wine"), classifier.poiTypes(tags("shop=wine;alcohol")));
        assertEquals(List.of("amenity-dive_centre"), classifier.poiTypes(tags("amenity=dive_centre;shelter")));
    }

    private static String categoryOf(String... tag) {
        return classifier.poiTags(tags(tag)).stream()
                .filter(poiTag -> poiTag.getKey().equals(OrganicMapsPoiClassifier.POI_CATEGORY_TAG))
                .findFirst().orElseThrow().getValue();
    }

    @Test
    void poiTypes_towers_onlyObservationBellTowerPagodaStupaAndChedi() {
        assertEquals(List.of("man_made-tower"), classifier.poiTypes(tags("man_made=tower", "tower:type=observation")));
        assertEquals(List.of("man_made-tower"), classifier.poiTypes(tags("man_made=tower", "tower:type=bell_tower")));
        assertEquals(List.of("man_made-tower"), classifier.poiTypes(tags("man_made=tower", "tower:type=pagoda")));
        assertEquals(List.of("man_made-tower"), classifier.poiTypes(tags("man_made=tower", "tower:type=stupa")));
        assertEquals(List.of("man_made-tower"), classifier.poiTypes(tags("man_made=tower", "tower:type=chedi")));
        assertEquals(List.of(), classifier.poiTags(tags("man_made=tower", "tower:type=pagoda")).stream()
                .filter(tag -> tag.getKey().equals(OrganicMapsPoiClassifier.POI_CATEGORY_TAG)).toList());
        assertEquals(List.of(), classifier.poiTypes(tags("man_made=tower")));
        assertEquals(List.of(), classifier.poiTypes(tags("man_made=tower", "tower:type=lighting")));
        assertEquals(List.of(), classifier.poiTypes(tags("man_made=tower", "tower:type=communication")));
        assertEquals(List.of(), classifier.poiTypes(tags("man_made=mast", "tower:type=communication")));
        assertEquals(List.of(), classifier.poiTypes(tags("man_made=communications_tower")));
    }

    @Test
    void poiTypes_notPoi() {
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=bench")));
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
