package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Paths;
import java.util.ArrayList;
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
        assertEquals(List.of("man_made-bridge"), classifier.poiTypes(tags("man_made=bridge", "name=Крымский мост")));
        assertEquals(List.of(), classifier.poiTypes(tags("man_made=bridge")));
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
    void poiTypes_privateParkingOfEveryKindIsNotPoi() {
        for (String access : List.of("access=private", "access=no")) {
            assertEquals(List.of(), classifier.poiTypes(tags("amenity=parking", access)));
            assertEquals(List.of(), classifier.poiTypes(tags("amenity=parking", "fee=yes", access)));
            for (String parking : List.of("multi-storey", "underground", "street_side", "lane", "surface")) {
                assertEquals(List.of(), classifier.poiTypes(tags("amenity=parking", "parking=" + parking, access)), parking);
                assertEquals(List.of(), classifier.poiTypes(tags("amenity=parking", "parking=" + parking, "fee=yes", access)), parking);
            }
        }
        assertEquals(List.of("amenity-parking-multi-storey"), classifier.poiTypes(tags("amenity=parking", "parking=multi-storey")));
        assertEquals(List.of("amenity-parking-lane-fee"), classifier.poiTypes(tags("amenity=parking", "parking=lane", "fee=yes")));
        assertEquals(List.of("amenity-parking-permissive", "amenity-parking-street_side"), classifier.poiTypes(tags("amenity=parking", "parking=street_side", "access=permissive")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=parking_space", "parking_space=disabled")));
    }

    @Test
    void poiTypes_otherPrivateObjectsAreNotPoi() {
        for (String access : List.of("access=private", "access=no")) {
            for (String tag : List.of("leisure=garden", "leisure=playground", "leisure=pitch", "leisure=fitness_station", "leisure=sports_centre", "leisure=marina", "sport=soccer",
                    "man_made=pier", "amenity=charging_station", "amenity=water_point", "amenity=drinking_water", "man_made=water_tap", "man_made=water_well")) {
                assertEquals(List.of(), classifier.poiTypes(tags(tag, access)), tag);
            }
            assertEquals(List.of(), classifier.poiTypes(tags("leisure=swimming_pool", "sport=swimming", access)));
            assertEquals(List.of("amenity-school"), classifier.poiTypes(tags("amenity=school", "sport=soccer", access)));
            for (String tag : List.of("amenity=cafe", "amenity=restaurant", "amenity=fast_food", "amenity=food_court", "amenity=bar", "amenity=pub",
                    "amenity=biergarten", "amenity=ice_cream", "amenity=canteen", "amenity=atm", "amenity=fuel", "tourism=picnic_site")) {
                assertEquals(List.of(), classifier.poiTypes(tags(tag, "name=Staff", access)), tag);
            }
        }
        assertEquals(List.of("amenity-cafe"), classifier.poiTypes(tags("amenity=cafe", "access=customers")));
        assertEquals(List.of("amenity-fuel"), classifier.poiTypes(tags("amenity=fuel")));
        assertEquals(List.of(), classifier.poiTypes(tags("leisure=garden", "garden:type=residential")));
        assertEquals(List.of("leisure-garden"), classifier.poiTypes(tags("leisure=garden")));
        assertEquals(List.of("leisure-pitch"), classifier.poiTypes(tags("leisure=pitch", "sport=soccer", "access=customers")));
    }

    @Test
    void poiTypes_excludedTypeRemovesItsGroup() {
        assertEquals(List.of(), classifier.poiTypes(tags("natural=water", "water=lake", "name=Lac", "intermittent=yes")));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=water", "water=pond", "name=Lac", "intermittent=yes")));
        assertEquals(List.of(), classifier.poiTypes(tags("aeroway=aerodrome", "aerodrome=international", "aerodrome:type=military", "name=Base")));
        assertEquals(List.of("shop-wholesale"), classifier.poiTypes(tags("landuse=industrial", "industrial=warehouse", "shop=wholesale", "name=Depot")));
        assertEquals(List.of("shop-car_repair"), classifier.poiTypes(tags("landuse=industrial", "industrial=depot", "shop=car_repair", "name=Depot")));
        assertEquals(List.of("landuse-industrial-warehouse"), classifier.poiTypes(tags("landuse=industrial", "industrial=warehouse", "name=Depot")));
        assertEquals(List.of("tourism-information-office"), classifier.poiTypes(tags("tourism=information", "information=board;office")));
        assertEquals(List.of("tourism-information-office", "tourism-information-visitor_centre"), classifier.poiTypes(tags("tourism=information", "information=office;visitor_centre")));
        assertEquals(List.of("tourism-museum"), classifier.poiTypes(tags("tourism=information;museum")));
        assertEquals(List.of("attraction-carousel"), classifier.poiTypes(tags("attraction=animal;carousel")));
        assertEquals(List.of("amenity-cafe"), classifier.poiTypes(tags("amenity=toilets;cafe")));
    }

    @Test
    void poiTypes_businessesAreNotDiveSites() {
        assertEquals(List.of("amenity-dive_centre", "sport-scuba_diving"), classifier.poiTypes(tags("amenity=dive_centre", "sport=scuba_diving;snorkeling")));
        assertEquals(List.of("amenity-dive_centre", "sport-scuba_diving"), classifier.poiTypes(tags("amenity=dive_centre", "sport=scuba_diving", "scuba_diving:entry:boat=yes")));
        assertEquals(List.of("amenity-dive_centre", "sport-scuba_diving"), classifier.poiTypes(tags("amenity=dive_centre", "sport=scuba_diving", "scuba_diving:type:cave=yes")));
        assertEquals(List.of("shop-scuba_diving"), classifier.poiTypes(tags("shop=scuba_diving", "diving:snorkeling=yes")));
        assertEquals(List.of("tourism-hotel"), classifier.poiTypes(tags("tourism=hotel", "scuba_diving:type:snorkeling=yes")));
        assertEquals(List.of("sport-scuba_diving", "tourism-guest_house"), classifier.poiTypes(tags("tourism=guest_house", "sport=scuba_diving", "scuba_diving:entry:shore=yes")));
        assertEquals(List.of("office-company", "sport-scuba_diving"), classifier.poiTypes(tags("office=company", "sport=scuba_diving", "scuba_diving:entry:boat=yes")));
        assertEquals(List.of("sport-snorkeling", "tourism-attraction"), classifier.poiTypes(tags("sport=snorkeling", "tourism=attraction")));
        assertEquals("{category_dive_site,category_tourism}", categoryOf("sport=snorkeling", "tourism=attraction"));
    }

    @Test
    void poiTypes_rentalShopAndTravelAgency() {
        assertEquals(List.of("shop-rental"), classifier.poiTypes(tags("shop=rental")));
        assertEquals(List.of("shop-travel_agency"), classifier.poiTypes(tags("shop=travel_agency")));
        assertEquals("{category_office_services,category_work}", categoryOf("shop=travel_agency"));
    }

    @Test
    void poiTypes_negativeFactorsAreNotPoi() {
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=prison", "name=Prison")));
        assertEquals(List.of(), classifier.poiTypes(tags("shop=funeral_directors", "name=Funeral")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=waste_transfer_station")));
    }

    @Test
    void getPoiTypes_everyTypeHasNamesInEveryLanguage() {
        for (String language : OrganicMapsPoiClassifier.NAME_LANGUAGES) {
            List<String> missing = new ArrayList<>();
            for (PoiType type : classifier.getPoiTypes()) {
                if (type.getName(language) == null) {
                    missing.add(type.getType());
                }
            }
            assertEquals(List.of(), missing, language);
        }
    }

    @Test
    void poiTypes_protectedArea_onlyNamedNatureClasses() {
        assertEquals(List.of("boundary-protected_area-2"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=2", "name=Park")));
        assertEquals(List.of("boundary-protected_area-5"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=5", "name=Park")));
        assertEquals(List.of("boundary-protected_area-1"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=1a", "name=Kernzone")));
        assertEquals(List.of("boundary-protected_area-4"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=4", "name=Naturschutzgebiet")));
        assertEquals(List.of("boundary-protected_area-6"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=6", "name=Reserve")));
        assertEquals(List.of("boundary-protected_area"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=7", "name=Заказник")));
        assertEquals(List.of("boundary-protected_area"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=97", "name=Flora-Fauna-Habitat")));
        assertEquals("{category_protected_area}", categoryOf("boundary=protected_area", "protect_class=7", "name=Заказник"));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=2")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=4")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=22", "name=Denkmalbereich")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=12", "name=Wasserschutzgebiet")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "name=Park")));
    }

    @Test
    void poiTypes_heritageArea_onlyWithWikiLink() {
        assertEquals(List.of("boundary-protected_area-22"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=22", "name=Weiße Stadt", "wikidata=Q2554180")));
        assertEquals(List.of("boundary-protected_area-22"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=22", "heritage=1",
                "name=Gartenstadt Falkenberg", "wikipedia=de:Gartenstadt Falkenberg")));
        assertEquals("{category_tourism}", categoryOf("boundary=protected_area", "protect_class=22", "name=Weiße Stadt", "wikidata=Q2554180"));
        assertEquals(List.of("boundary-protected_area-22", "leisure-park"), classifier.poiTypes(tags("boundary=protected_area", "protect_class=22",
                "leisure=park", "name=Schillerpark", "wikidata=Q2240370")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=22", "wikidata=Q2554180")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=22", "heritage=4", "name=Wohnanlage Becherweg")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=22", "heritage=1",
                "name=Московский Кремль и Красная Площадь. Буферная зона")));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=12", "name=Wasserschutzgebiet", "wikidata=Q1")));
    }

    @Test
    void poiTypes_scenariosLeftOutAreNotPoi() {
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=toilets", "name=WC")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=toilets", "access=yes", "fee=no")));
        assertEquals(List.of(), classifier.poiTypes(tags("route=mtb", "name=Trail")));
        assertEquals(List.of(), classifier.poiTypes(tags("route=bicycle", "name=Radweg")));
        assertEquals(List.of(), classifier.poiTypes(tags("aerialway=zip_line", "name=Zip Line")));
        for (String sport : List.of("mtb", "bmx", "atv")) {
            assertEquals(List.of(), classifier.poiTypes(tags("sport=" + sport, "name=Park")), sport);
        }
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=farmyard", "name=Farm")));
        assertEquals(List.of(), classifier.poiTypes(tags("place=farm", "name=Le Gouet")));
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=allotments", "name=Kolonie")));
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=allotments", "club=allotments", "name=Ernst-Ludwig Heim")));
        assertEquals(List.of("amenity-community_centre"), classifier.poiTypes(tags("amenity=community_centre", "club=allotments", "name=Vereinshaus")));
        assertEquals(List.of(), classifier.poiTypes(tags("emergency=assembly_point", "name=Tsunami Assembly Point")));
        assertEquals(List.of(), classifier.poiTypes(tags("railway=narrow_gauge", "name=Train du Montenvers")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=mobility_hub", "name=Jelbi")));
        assertEquals(List.of(), classifier.poiTypes(tags("route=ferry", "name=Phuket - Koh Yao Noi")));
        assertEquals(List.of(), classifier.poiTypes(tags("military=office", "name=Военный комиссариат")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=public_building", "name=КЦСО")));
    }

    @Test
    void poiTags_ferryTerminal_seaTransportInsteadOfTransport() {
        assertEquals(List.of("amenity-ferry_terminal"), classifier.poiTypes(tags("amenity=ferry_terminal", "name=Rassada Pier")));
        assertEquals("{category_sea_transport}", categoryOf("amenity=ferry_terminal", "name=Rassada Pier"));
        assertEquals("{category_transport}", categoryOf("amenity=bus_station"));
    }

    @Test
    void poiTags_familyAndChildren() {
        assertEquals(List.of("club"), classifier.poiTypes(tags("club=scout", "name=Pfadfinder")));
        assertEquals("{category_community_centre}", categoryOf("club=chess", "name=Шахматный клуб"));
        assertEquals(List.of(), classifier.poiTypes(tags("club=youth")));
        assertEquals(List.of("sport-club"), classifier.poiTypes(tags("club=sport", "name=SV Blau-Weiss")));
        assertEquals(List.of("amenity-internet_cafe"), classifier.poiTypes(tags("club=computer", "name=Colizeum")));
        assertEquals(List.of(), classifier.poiTypes(tags("club=swinger", "name=Club")));
        assertEquals(List.of("amenity-community_centre", "club"), classifier.poiTypes(tags("amenity=community_centre", "club=culture", "name=Haus")));
        assertEquals(List.of("leisure-trampoline_park"), classifier.poiTypes(tags("leisure=trampoline_park", "name=Jump House")));
        assertEquals("{category_children}", categoryOf("leisure=trampoline_park"));
        assertEquals(List.of("amenity-traffic_park"), classifier.poiTypes(tags("amenity=traffic_park", "name=Jugendverkehrsschule")));
        assertEquals("{category_children}", categoryOf("amenity=traffic_park"));
    }

    @Test
    void poiTags_housingSearchAndMedicine() {
        assertEquals(List.of("amenity-veterinary_pharmacy"), classifier.poiTypes(tags("amenity=veterinary_pharmacy", "name=Ветаптека")));
        assertEquals("{category_pet_services,category_pets}", categoryOf("amenity=veterinary_pharmacy"));
        assertEquals(List.of("amenity-motorcycle_parking"), classifier.poiTypes(tags("amenity=motorcycle_parking")));
        assertEquals("{category_parking}", categoryOf("amenity=motorcycle_parking"));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=motorcycle_parking", "access=private")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=motorcycle_parking", "access=no")));
        assertEquals(List.of("amenity-doctors"), classifier.poiTypes(tags("office=physician", "name=Dr. Weber")));
        assertEquals(List.of("healthcare-other"), classifier.poiTypes(tags("office=therapist", "name=Praxis")));
        assertEquals(List.of("healthcare-other"), classifier.poiTypes(tags("office=midwife", "name=Hebamme")));
        assertEquals(List.of("healthcare-psychotherapist"), classifier.poiTypes(tags("office=psychologist", "name=Psychologin")));
        assertEquals("{category_health}", categoryOf("office=psychologist"));
        assertEquals(List.of("office"), classifier.poiTypes(tags("office=it")));
        assertEquals(List.of("emergency-ambulance_station"), classifier.poiTypes(tags("emergency=ambulance_station", "name=Станция скорой помощи")));
        assertEquals("{category_rescue}", categoryOf("emergency=ambulance_station"));
    }

    @Test
    void poiTags_educationAndKindergartenBuildings() {
        for (String school : List.of("sailing_school", "surf_school", "sport_school", "flight_school")) {
            assertEquals(List.of("amenity-training"), classifier.poiTypes(tags("amenity=" + school, "name=School")), school);
        }
        assertEquals("{category_courses}", categoryOf("amenity=sailing_school"));
        assertEquals(List.of("building-kindergarten"), classifier.poiTypes(tags("building=kindergarten", "name=Детский сад №1243")));
        assertEquals("{category_kindergarten}", categoryOf("building=kindergarten", "name=Kita"));
        assertEquals(List.of(), classifier.poiTypes(tags("building=kindergarten")));
        assertEquals(List.of("amenity-kindergarten"), classifier.poiTypes(tags("building=kindergarten", "amenity=kindergarten", "name=Kita")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=school", "name=Школа 1008")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=hospital", "name=Haus 4")));
    }

    @Test
    void poiTags_placesOfWork_commercialBuildingsAndRetailAreas() {
        assertEquals(List.of("building-commercial"), classifier.poiTypes(tags("building=commercial", "name=Бизнес-центр Сокол")));
        assertEquals("{category_business_centre,category_work}", categoryOf("building=commercial", "name=Atrium Tower"));
        assertEquals(List.of(), classifier.poiTypes(tags("building=commercial")));
        assertEquals(List.of("shop-mall"), classifier.poiTypes(tags("building=commercial", "shop=mall", "name=Mall")));
        assertEquals(List.of("office-company"), classifier.poiTypes(tags("building=commercial", "office=company", "name=Zalando")));
        assertEquals(List.of("man_made-works"), classifier.poiTypes(tags("building=commercial", "man_made=works", "name=Peter-Behrens-Hallen")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=industrial", "name=Котельная")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=warehouse", "name=Halle 7")));
        assertEquals(List.of("landuse-retail"), classifier.poiTypes(tags("landuse=retail", "name=Торговый центр")));
        assertEquals("{category_commercial_area,category_work}", categoryOf("landuse=retail", "name=Market"));
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=retail")));
        assertEquals(List.of("shop-mall"), classifier.poiTypes(tags("landuse=retail", "shop=mall", "name=Mall")));
    }

    @Test
    void poiTags_seaFishingAndHunting() {
        assertEquals(List.of("place-archipelago"), classifier.poiTypes(tags("place=archipelago", "name=Phi Phi Islands")));
        assertEquals(List.of("natural-atoll"), classifier.poiTypes(tags("natural=atoll", "name=Ari Atoll")));
        assertEquals("{category_coast}", categoryOf("natural=atoll"));
        assertEquals(List.of("landuse-harbour"), classifier.poiTypes(tags("landuse=harbour", "name=Thoddoo Harbour")));
        assertEquals("{category_pier}", categoryOf("landuse=harbour", "name=Thoddoo Harbour"));
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=harbour")));
        assertEquals(List.of("shop-boat", "shop-water_sports"), classifier.poiTypes(tags("shop=boat;water_sports", "name=Boat Shop")));
        assertEquals("{category_water_activities}", categoryOf("shop=surf"));
        assertEquals(List.of("club-fishing"), classifier.poiTypes(tags("club=fishing", "name=Angelverein")));
        assertEquals("{category_water_activities}", categoryOf("club=fishing", "name=Angelverein"));
        assertEquals(List.of("shop-hunting"), classifier.poiTypes(tags("shop=hunting", "name=Охотник")));
        assertEquals("{category_hunting}", categoryOf("shop=hunting"));
        assertEquals(List.of("club-hunting"), classifier.poiTypes(tags("club=hunting", "name=Jagdverein")));
        assertEquals(List.of("whitewater-put_in"), classifier.poiTypes(tags("whitewater=put_in;egress")));
        assertEquals("{category_water_sports}", categoryOf("whitewater=egress"));
    }

    @Test
    void poiTags_mountainsAndNature() {
        assertEquals(List.of("attraction-alpine_coaster", "tourism-attraction"), classifier.poiTypes(tags("attraction=alpine_coaster", "tourism=attraction", "name=Luge Alpine Coaster")));
        assertEquals(List.of(), classifier.poiTypes(tags("attraction=alpine_coaster", "roller_coaster=track", "name=Luge Alpine Coaster")));
        assertEquals(List.of("sport-climbing"), classifier.poiTypes(tags("climbing=crag", "name=Secteur du Haut")));
        assertEquals(List.of("natural-cliff-notable"), classifier.poiTypes(tags("natural=cliff", "climbing=crag")));
        assertEquals(List.of("amenity-shelter-weather_shelter"), classifier.poiTypes(tags("amenity=shelter", "shelter_type=weather_shelter", "name=Abri du Pécleret")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=shelter", "shelter_type=weather_shelter")));
        assertEquals(List.of("amenity-shelter-rock_shelter"), classifier.poiTypes(tags("amenity=shelter", "shelter_type=rock_shelter")));
        assertEquals("{category_hut}", categoryOf("amenity=shelter", "shelter_type=rock_shelter"));
        assertEquals(List.of("landuse-forest"), classifier.poiTypes(tags("landuse=forest", "name=Tegeler Forst")));
        assertEquals(List.of("landuse-forest-mixed"), classifier.poiTypes(tags("natural=wood", "leaf_type=mixed", "name=Битцевский лес")));
        assertEquals("{category_forest}", categoryOf("natural=wood", "name=Bamboo Forest"));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=wood")));
        assertEquals(List.of("place-islet"), classifier.poiTypes(tags("place=islet", "natural=wood", "name=Lindwerder")));
        assertEquals(List.of("geological"), classifier.poiTypes(tags("geological=outcrop", "name=Felsen")));
        assertEquals("{category_geotourism}", categoryOf("geological=outcrop", "name=Felsen"));
        assertEquals(List.of(), classifier.poiTypes(tags("geological=moraine")));
        assertEquals(List.of("geological-palaeontological_site"), classifier.poiTypes(tags("geological=palaeontological_site")));
        assertEquals(List.of("waterway-rapids"), classifier.poiTypes(tags("waterway=rapids", "name=Порог Ревун")));
        assertEquals(List.of(), classifier.poiTypes(tags("waterway=rapids")));
    }

    @Test
    void poiTags_historyCultureGastronomyAndCrafts() {
        assertEquals(List.of("historic"), classifier.poiTypes(tags("historic=optical_telegraph", "name=Optischer Telegraf")));
        assertEquals("{category_tourism}", categoryOf("historic=church", "name=Dorfkirche"));
        assertEquals(List.of(), classifier.poiTypes(tags("historic=church")));
        assertEquals(List.of(), classifier.poiTypes(tags("historic=railway", "name=Siemensbahn")));
        assertEquals(List.of(), classifier.poiTypes(tags("historic=neighbourhood", "name=Зарядье")));
        assertEquals(List.of(), classifier.poiTypes(tags("historic=bomb_crater", "name=Krater")));
        assertEquals(List.of("historic-castle"), classifier.poiTypes(tags("historic=castle")));
        assertEquals(List.of("historic-ruins"), classifier.poiTypes(tags("historic=ruins")));
        assertEquals(List.of("historic-memorial"), classifier.poiTypes(tags("historic=memorial")));
        assertEquals(List.of("historic-archaeological_site"), classifier.poiTypes(tags("historic=archaeological_site")));
        assertEquals(List.of("man_made-adit"), classifier.poiTypes(tags("man_made=adit", "name=Mines des Baraques")));
        assertEquals(List.of(), classifier.poiTypes(tags("man_made=adit")));
        assertEquals(List.of("man_made-watermill"), classifier.poiTypes(tags("man_made=watermill")));
        assertEquals(List.of(), classifier.poiTypes(tags("man_made=mineshaft", "name=ВШМ 658")));
        assertEquals(List.of("waterway-dam"), classifier.poiTypes(tags("waterway=dam", "name=Bang Neaw Dam")));
        assertEquals("{category_landmark}", categoryOf("waterway=dam", "name=Bang Neaw Dam"));
        assertEquals(List.of(), classifier.poiTypes(tags("waterway=dam")));
        assertEquals(List.of("landuse-religious"), classifier.poiTypes(tags("landuse=religious", "name=Wat Ket Ho")));
        assertEquals("{category_worship}", categoryOf("landuse=religious", "name=Wat Ket Ho"));
        assertEquals(List.of("amenity-place_of_worship"), classifier.poiTypes(tags("landuse=religious", "amenity=place_of_worship", "name=Wat")));
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=religious")));
        assertEquals(List.of("amenity-canteen"), classifier.poiTypes(tags("amenity=canteen", "name=Mensa")));
        assertEquals("{category_eat}", categoryOf("amenity=canteen"));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=canteen", "access=private")));
        for (String shop : List.of("spices", "honey", "nuts", "frozen_food")) {
            assertEquals(List.of("shop-" + shop), classifier.poiTypes(tags("shop=" + shop)), shop);
            assertEquals("{category_food}", categoryOf("shop=" + shop), shop);
        }
        assertEquals(List.of("craft"), classifier.poiTypes(tags("craft=window_construction", "name=Экоокна")));
        assertEquals("{category_trades,category_work}", categoryOf("craft=glaziery"));
        assertEquals(List.of("craft-carpenter"), classifier.poiTypes(tags("craft=carpenter")));
        assertEquals(List.of(), classifier.poiTypes(tags("craft=sculptor", "name=Atelier")));
        assertEquals(List.of(), classifier.poiTypes(tags("craft=stonemason", "name=Памятники")));
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
        assertEquals("{category_college_university,category_university}", categoryOf("amenity=university"));
        assertEquals("{category_college,category_college_university}", categoryOf("amenity=college"));
        assertEquals(List.of("building-university"), classifier.poiTypes(tags("building=university", "name=Main Hall")));
        assertEquals(List.of("building-college"), classifier.poiTypes(tags("building=college", "name=Workshop")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=university")));
        assertEquals(List.of("amenity-university"), classifier.poiTypes(tags("building=university", "amenity=university", "name=Campus")));
        assertEquals(List.of("amenity-research_institute"), classifier.poiTypes(tags("building=university", "amenity=research_institute", "name=Lab")));
        assertEquals(List.of("office-educational_institution"),
                classifier.poiTypes(tags("building=university", "office=educational_institution", "name=Office")));
        assertEquals("{category_college,category_college_university}", categoryOf("building=college", "name=Workshop"));
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
        assertEquals("{category_airport,category_transport}", categoryOf("aeroway=aerodrome", "name=Megeve"));
    }

    @Test
    void poiTags_divingAndAerialwayStations_ownCategories() {
        assertEquals(List.of("sport-scuba_diving"), classifier.poiTypes(tags("sport=scuba_diving")));
        assertEquals(List.of("sport-scuba_diving-divespot"), classifier.poiTypes(tags("scuba_diving:divespot=yes")));
        assertEquals(List.of("amenity-dive_centre"), classifier.poiTypes(tags("amenity=dive_centre")));
        assertEquals(List.of("shop-scuba_diving"), classifier.poiTypes(tags("shop=scuba_diving")));
        assertEquals(List.of("aerialway-station"), classifier.poiTypes(tags("aerialway=station")));
        assertEquals(List.of(), classifier.poiTypes(tags("aerialway=gondola")));

        assertEquals("{category_diving,category_sport}", categoryOf("sport=scuba_diving"));
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
        assertEquals("{category_dive_site}", categoryOf("natural=reef"));
    }

    @Test
    void poiTags_wreckIsDiveSiteOnlyWithDivingMark() {
        assertEquals(List.of(), classifier.poiTypes(tags("historic=wreck")));
        assertEquals(List.of(), classifier.poiTypes(tags("seamark:type=wreck", "seamark:wreck:category=hull_showing")));
        assertEquals(List.of("historic-wreck"), classifier.poiTypes(tags("seamark:type=wreck", "name=Old Barge")));
        assertEquals("{category_tourism}", categoryOf("historic=wreck", "name=Old Barge"));
        assertEquals(List.of("sport-scuba_diving-divespot"), classifier.poiTypes(tags("historic=wreck", "sport=scuba_diving")));
        assertEquals(List.of("sport-scuba_diving-divespot"), classifier.poiTypes(tags("seamark:type=wreck", "sport=scuba_diving")));
        assertEquals(List.of("historic-wreck", "sport-scuba_diving-divespot"),
                classifier.poiTypes(tags("historic=wreck", "seamark:type=wreck", "sport=scuba_diving", "name=Victory")));
        assertEquals("{category_dive_site,category_diving,category_tourism}", categoryOf("historic=wreck", "sport=scuba_diving", "name=Victory"));
        assertEquals(List.of("amenity-dive_centre", "historic-wreck", "sport-scuba_diving"),
                classifier.poiTypes(tags("amenity=dive_centre", "historic=wreck", "sport=scuba_diving", "name=Wreck Divers")));
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
        assertEquals("{category_clinic,category_hospital,category_trauma}", categoryOf("amenity=clinic", "healthcare:speciality=neurology;trauma"));
        assertEquals("{category_trauma}", categoryOf("healthcare:speciality=trauma"));
        assertEquals(List.of("emergency-emergency_ward_entrance"), classifier.poiTypes(tags("emergency=emergency_ward_entrance")));
    }

    @Test
    void poiTypes_cultureTags() {
        assertEquals(List.of("historic-yes"), classifier.poiTypes(tags("historic=yes")));
        assertEquals("{category_tourism}", categoryOf("historic=monastery"));
        assertEquals("{category_tourism}", categoryOf("historic=heritage"));
        assertEquals(List.of("historic-vehicle"), classifier.poiTypes(tags("historic=railway_car")));
        assertEquals(List.of("historic-military"), classifier.poiTypes(tags("historic=bunker", "name=Bunker 42")));
        assertEquals("{category_tourism}", categoryOf("historic=bunker", "name=Bunker 42"));
        assertEquals(List.of(), classifier.poiTypes(tags("historic=bunker")));
        assertEquals(List.of(), classifier.poiTypes(tags("historic=bomb_crater")));
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
        assertEquals(List.of("sport-martial_arts-school"), classifier.poiTypes(tags("sport=muaythai")));
        assertEquals(List.of("sport-ice_skating"), classifier.poiTypes(tags("sport=ice_skating")));
        assertEquals(List.of("sport-winter_sports"), classifier.poiTypes(tags("sport=biathlon")));
        assertEquals(List.of("sport-indoor_games"), classifier.poiTypes(tags("sport=billiards")));
        assertEquals(List.of("sport-soccer"), classifier.poiTypes(tags("sport=football")));
        assertEquals(List.of("sport-ice_hockey"), classifier.poiTypes(tags("sport=ice_hockey")));
        assertEquals(List.of("sport-multi"), classifier.poiTypes(tags("sport=multi")));
        assertEquals(List.of("sport-club"), classifier.poiTypes(tags("club=sport")));
    }

    @Test
    void poiTags_animalPlaces_petsCategory() {
        assertEquals("{category_pet_services,category_pets}", categoryOf("amenity=veterinary"));
        assertEquals("{category_dog_areas,category_pets}", categoryOf("leisure=dog_park"));
        assertEquals("{category_pet_services,category_pets}", categoryOf("shop=pet_grooming"));
        assertEquals("{category_dog_areas,category_pets}", categoryOf("amenity=animal_training"));
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
        assertEquals("{category_landmark}", categoryOf("man_made=tower", "tower:type=pagoda"));
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
    void getPoiTypes_everyTypeHasACategory() {
        List<String> withoutCategory = classifier.getPoiTypes().stream().filter(poiType -> poiType.getCategories().isEmpty()).map(PoiType::getType).toList();
        assertEquals(List.of("emergency-emergency_ward_entrance"), withoutCategory);
    }

    @Test
    void poiTags_sportIsSplitIntoFitnessPitchesPoolsAndOtherSport() {
        assertEquals(List.of("sport-fitness"), classifier.poiTypes(tags("sport=fitness")));
        assertEquals("{category_fitness}", categoryOf("sport=fitness"));
        assertEquals(List.of("leisure-fitness_station"), classifier.poiTypes(tags("leisure=fitness_station", "sport=fitness")));
        assertEquals("{category_pitch}", categoryOf("leisure=fitness_station", "sport=fitness"));
        assertEquals(List.of("leisure-fitness_centre"), classifier.poiTypes(tags("leisure=fitness_centre", "sport=fitness")));
        assertEquals("{category_fitness}", categoryOf("leisure=fitness_centre"));
        assertEquals("{category_fitness}", categoryOf("amenity=dojo"));
        assertEquals("{category_fitness}", categoryOf("sport=yoga"));
        assertEquals("{category_fitness}", categoryOf("sport=boxing"));
        assertEquals(List.of("sport-indoor_games"), classifier.poiTypes(tags("sport=billiards")));
        assertEquals("{category_sport}", categoryOf("sport=billiards"));
        assertEquals("{category_pitch}", categoryOf("leisure=pitch", "sport=soccer"));
        assertEquals("{category_pitch}", categoryOf("leisure=track", "area=yes"));
        assertEquals("{category_pool}", categoryOf("leisure=swimming_pool"));
        assertEquals("{category_sport,category_sports_centre}", categoryOf("leisure=sports_centre"));
        assertEquals("{category_water_sports}", categoryOf("sport=surfing"));
        assertEquals("{category_air_sports}", categoryOf("sport=free_flying"));
    }

    @Test
    void poiTags_natureIsSplitIntoProtectedAreasCoastAndMountains() {
        assertEquals("{category_protected_area,category_tourism}", categoryOf("boundary=national_park"));
        assertEquals("{category_protected_area,category_tourism}", categoryOf("leisure=nature_reserve"));
        assertEquals("{category_protected_area}", categoryOf("boundary=protected_area", "protect_class=2", "name=Park"));
        assertEquals(List.of(), classifier.poiTypes(tags("boundary=protected_area", "protect_class=2")));
        assertEquals("{category_coast}", categoryOf("place=island"));
        assertEquals("{category_coast}", categoryOf("natural=bay"));
        assertEquals("{category_mountain}", categoryOf("mountain_pass=yes"));
        assertEquals("{category_mountain}", categoryOf("natural=saddle"));
        assertEquals("{category_mountain}", categoryOf("natural=cliff", "name=Aiguille"));
        assertEquals("{category_mountain,category_tourism}", categoryOf("natural=glacier", "name=Mer de Glace"));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=glacier")));
        assertEquals("{category_geotourism}", categoryOf("natural=cave_entrance"));
    }

    @Test
    void poiTags_servicesAndLeisureAreSplitByMeaning() {
        assertEquals("{category_pier}", categoryOf("man_made=pier"));
        assertEquals("{category_water_activities}", categoryOf("leisure=marina"));
        assertEquals("{category_water_activities}", categoryOf("shop=fishing"));
        assertEquals("{category_events}", categoryOf("amenity=conference_centre"));
        assertEquals("{category_gambling}", categoryOf("shop=bookmaker"));
        assertEquals("{category_gambling,category_nightlife}", categoryOf("amenity=casino"));
        assertEquals("{category_adult}", categoryOf("amenity=stripclub"));
        assertEquals("{category_entertainment}", categoryOf("leisure=escape_game"));
        assertEquals("{category_community_centre}", categoryOf("leisure=hackerspace"));
        assertEquals("{category_wellness}", categoryOf("leisure=spa"));
        assertEquals("{category_wellness}", categoryOf("shop=massage"));
        assertEquals("{category_beauty}", categoryOf("shop=hairdresser"));
        assertEquals("{category_camping,category_hotel}", categoryOf("tourism=camp_site"));
        assertEquals("{category_camping,category_hotel,category_rv}", categoryOf("tourism=caravan_site"));
        assertEquals("{category_eat}", categoryOf("amenity=food_court"));
        assertEquals("{category_craft_drink}", categoryOf("craft=winery"));
        assertEquals("{category_craft_food}", categoryOf("craft=bakery"));
        assertEquals("{category_craft_food}", categoryOf("craft=beekeeper"));
        assertEquals("{category_crafts}", categoryOf("craft=handicraft"));
        assertEquals("{category_airport}", categoryOf("aeroway=terminal"));
        assertEquals("{category_aerialway}", categoryOf("railway=station", "station=funicular"));
        assertEquals("{category_services}", categoryOf("amenity=luggage_locker"));
        assertEquals("{category_road}", categoryOf("amenity=charging_station"));
        assertEquals("{category_library}", categoryOf("amenity=library"));
        assertEquals("{category_library}", categoryOf("amenity=public_bookcase"));
        assertEquals("{category_rescue}", categoryOf("amenity=fire_station"));
        assertEquals("{category_rescue}", categoryOf("emergency=mountain_rescue"));
        assertEquals("{category_social}", categoryOf("amenity=nursing_home"));
        assertEquals("{category_outdoor}", categoryOf("amenity=bbq"));
        assertEquals("{category_small_history}", categoryOf("historic=wayside_shrine"));
        assertEquals("{category_worship}", categoryOf("amenity=place_of_worship", "religion=buddhist"));
        assertEquals("{category_commercial_area,category_work}", categoryOf("landuse=commercial", "name=Market"));
    }

    @Test
    void poiTags_everyShopHasACategoryOfItsOwn() {
        assertEquals(List.of("shop"), classifier.poiTypes(tags("shop=yes")));
        assertEquals("{category_shopping}", categoryOf("shop=yes"));
        assertEquals("{category_shopping}", categoryOf("shop=furniture"));
        assertEquals("{category_rental}", categoryOf("shop=rental"));
        assertEquals("{category_shopping}", categoryOf("shop=clothes"));
        assertEquals("{category_shopping}", categoryOf("shop=clothes", "clothes=women"));
        assertEquals("{category_food}", categoryOf("shop=supermarket"));
        assertEquals("{category_shopping}", categoryOf("shop=toys"));
    }

    @Test
    void poiTags_containersGiveWayToTheirOwnTypes() {
        assertEquals(List.of("office-government"), classifier.poiTypes(tags("building=office", "office=government", "name=Рособрнадзор")));
        assertEquals(List.of("office-research"), classifier.poiTypes(tags("landuse=commercial", "office=research", "name=Технопарк")));
        assertEquals(List.of("craft"), classifier.poiTypes(tags("landuse=commercial", "craft=roofer", "name=Melzer Bau GmbH")));
        assertEquals(List.of("man_made-works"), classifier.poiTypes(tags("landuse=industrial", "man_made=works", "name=Coca-Cola HBC")));
        assertEquals(List.of("landuse-industrial"), classifier.poiTypes(tags("landuse=industrial", "man_made=pumping_station", "name=Pumpwerk")));
        assertEquals(List.of("building-commercial"), classifier.poiTypes(tags("building=commercial", "man_made=water_tower", "name=Wasserturm")));
        assertEquals(List.of("amenity-place_of_worship"), classifier.poiTypes(tags("landuse=religious", "amenity=place_of_worship", "name=Wat")));
        assertEquals(List.of("office-religion"), classifier.poiTypes(tags("landuse=religious", "office=religion", "name=Патриаршая резиденция")));
    }

    @Test
    void poiTypes_industrialAreasOfWastewaterAndWaterWorksAreNotPoi() {
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=industrial", "man_made=wastewater_plant", "name=Klärwerk Ruhleben")));
        assertEquals(List.of(), classifier.poiTypes(tags("landuse=industrial", "man_made=water_works", "name=Wasserwerk Kaulsdorf")));
        assertEquals(List.of("landuse-industrial"), classifier.poiTypes(tags("landuse=industrial", "name=Plant")));
    }

    @Test
    void poiTypes_sportOfPitchesStaysOnlyForSportsPeopleTravelFor() {
        assertEquals(List.of("leisure-pitch"), classifier.poiTypes(tags("leisure=pitch", "sport=tennis")));
        assertEquals(List.of("leisure-pitch"), classifier.poiTypes(tags("leisure=pitch", "sport=table_tennis")));
        assertEquals(List.of("leisure-pitch"), classifier.poiTypes(tags("leisure=pitch", "sport=basketball;soccer")));
        assertEquals(List.of("leisure-track"), classifier.poiTypes(tags("leisure=track", "sport=running")));
        assertEquals(List.of("leisure-fitness_station"), classifier.poiTypes(tags("leisure=fitness_station", "sport=yoga")));
        assertEquals(List.of("leisure-pitch", "sport-equestrian"), classifier.poiTypes(tags("leisure=pitch", "sport=equestrian")));
        assertEquals(List.of("leisure-pitch", "sport-air_sports"), classifier.poiTypes(tags("leisure=pitch", "sport=free_flying")));
        assertEquals(List.of("leisure-pitch", "sport-shooting"), classifier.poiTypes(tags("leisure=pitch", "sport=shooting")));
        assertEquals(List.of("leisure-pitch", "sport-climbing"), classifier.poiTypes(tags("leisure=pitch", "sport=climbing")));
        assertEquals(List.of("leisure-track", "sport-karting"), classifier.poiTypes(tags("leisure=track", "sport=karting")));
        assertEquals("{category_pitch,category_sport}", categoryOf("leisure=pitch", "sport=equestrian"));
        assertEquals(List.of("leisure-sports_centre", "sport-tennis"), classifier.poiTypes(tags("leisure=sports_centre", "sport=tennis")));
        assertEquals(List.of("leisure-stadium", "sport-soccer"), classifier.poiTypes(tags("leisure=stadium", "sport=soccer")));
        assertEquals(List.of("sport-tennis"), classifier.poiTypes(tags("sport=tennis", "name=Tennis Club")));
        assertEquals(List.of("leisure-picnic_table"), classifier.poiTypes(tags("leisure=picnic_table", "sport=chess")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=table", "sport=chess")));
        assertEquals(List.of(), classifier.poiTypes(tags("amenity=table", "sport=table_tennis")));
        assertEquals(List.of("sport-chess"), classifier.poiTypes(tags("sport=chess", "name=Шахматный клуб")));
        assertEquals(List.of("leisure-pitch"), classifier.poiTypes(tags("leisure=pitch", "sport=ice_hockey;ice_skating")));
        assertEquals(List.of("leisure-ice_rink", "sport-ice_skating"), classifier.poiTypes(tags("leisure=ice_rink", "sport=ice_skating")));
        assertEquals(List.of("leisure-pitch", "sport-winter_sports"), classifier.poiTypes(tags("leisure=pitch", "sport=ski_jumping")));
        assertEquals(List.of("leisure-swimming_pool"), classifier.poiTypes(tags("leisure=swimming_pool", "sport=swimming")));
        assertEquals(List.of("leisure-swimming_area"), classifier.poiTypes(tags("leisure=swimming_area", "sport=swimming")));
        assertEquals(List.of("leisure-sports_centre", "sport-swimming"), classifier.poiTypes(tags("leisure=sports_centre", "sport=swimming")));
    }

    @Test
    void poiTypes_studiosOfYogaAndMartialArtsAreSeparateTypes() {
        assertEquals(List.of("sport-yoga-studio"), classifier.poiTypes(tags("sport=yoga", "name=Rama Yoga")));
        assertEquals("{category_fitness}", categoryOf("sport=yoga", "name=Rama Yoga"));
        assertEquals(List.of("sport-martial_arts-school"), classifier.poiTypes(tags("sport=judo", "name=Мир Самбо")));
        assertEquals(List.of("leisure-sports_hall", "sport-martial_arts"), classifier.poiTypes(tags("leisure=sports_hall", "sport=judo")));
        assertEquals(List.of("leisure-fitness_centre", "sport-yoga"), classifier.poiTypes(tags("leisure=fitness_centre", "sport=yoga")));
        assertEquals(List.of("amenity-dojo", "sport-martial_arts-school"), classifier.poiTypes(tags("amenity=dojo", "sport=karate")));
    }

    @Test
    void poiTags_servicesAreSplitIntoEverydayServicesCarsRentalTradesAndShops() {
        assertEquals("{category_services}", categoryOf("shop=laundry"));
        assertEquals("{category_services}", categoryOf("shop=dry_cleaning"));
        assertEquals("{category_services}", categoryOf("craft=tailor"));
        assertEquals("{category_services}", categoryOf("craft=key_cutter"));
        assertEquals("{category_services}", categoryOf("amenity=bureau_de_change"));
        assertEquals("{category_shopping}", categoryOf("shop=florist"));
        assertEquals("{category_shopping}", categoryOf("shop=mobile_phone"));
        assertEquals("{category_shopping}", categoryOf("shop=outdoor"));
        assertEquals("{category_car_services}", categoryOf("shop=car_repair"));
        assertEquals("{category_car_services}", categoryOf("shop=tyres"));
        assertEquals("{category_car_services}", categoryOf("amenity=car_wash"));
        assertEquals("{category_car_services}", categoryOf("shop=motorcycle"));
        assertEquals("{category_rental}", categoryOf("amenity=car_rental"));
        assertEquals("{category_rental}", categoryOf("amenity=motorcycle_rental"));
        assertEquals("{category_rental}", categoryOf("amenity=scooter_rental"));
        assertEquals("{category_rental}", categoryOf("amenity=bicycle_rental"));
        assertEquals("{category_trades,category_work}", categoryOf("craft=plumber"));
        assertEquals("{category_trades,category_work}", categoryOf("craft=boatbuilder"));
        assertEquals("{category_crafts}", categoryOf("craft=jeweller"));
        assertEquals("{category_food}", categoryOf("shop=tea"));
        assertEquals("{category_food}", categoryOf("shop=pasta"));
        assertEquals("{category_driving_school}", categoryOf("amenity=driving_school"));
        assertEquals("{category_trades,category_work}", categoryOf("craft=caterer"));
        assertEquals("{category_car_services}", categoryOf("amenity=vehicle_inspection"));
        assertEquals("{category_pickup}", categoryOf("amenity=parcel_locker"));
        assertEquals("{category_post}", categoryOf("amenity=post_office"));
    }

    @Test
    void poiTags_childrenLeisureFountainsHospitalsAndBridges() {
        assertEquals("{category_playground}", categoryOf("leisure=playground"));
        assertEquals("{category_shopping}", categoryOf("shop=baby_goods"));
        assertTrue(categoryOf("tourism=zoo").contains("category_children"));
        assertEquals("{category_children}", categoryOf("leisure=indoor_play"));
        assertEquals("{category_fountain}", categoryOf("amenity=fountain"));
        assertEquals("{category_hospital,category_inpatient}", categoryOf("amenity=hospital"));
        assertEquals("{category_clinic,category_hospital}", categoryOf("amenity=clinic"));
        assertEquals("{category_hospital}", categoryOf("amenity=doctors"));
        assertEquals("{category_landmark}", categoryOf("man_made=bridge", "name=Pont du Diable"));
    }

    @Test
    void poiTypes_namedBuildingsOfHotelsPlacesOfWorshipAndShops() {
        assertEquals(List.of("building-hotel"), classifier.poiTypes(tags("building=hotel", "name=Deva Suites Hotel")));
        assertEquals("{category_hotel}", categoryOf("building=hotel", "name=Deva Suites Hotel"));
        assertEquals(List.of("tourism-hotel"), classifier.poiTypes(tags("building=hotel", "tourism=hotel", "name=Hotel")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=hotel")));
        assertEquals(List.of("building-place_of_worship"), classifier.poiTypes(tags("building=mosque", "name=Hukuru Miskiy")));
        assertEquals("{category_worship}", categoryOf("building=church", "name=Dorfkirche Schöneberg"));
        assertEquals(List.of("amenity-place_of_worship"), classifier.poiTypes(tags("building=church", "amenity=place_of_worship", "name=Kirche")));
        assertEquals(List.of("tourism-museum"), classifier.poiTypes(tags("building=church", "tourism=museum", "name=Friedrichswerdersche Kirche")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=chapel", "name=Friedhofskapelle")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=church")));
        assertEquals(List.of("building-retail"), classifier.poiTypes(tags("building=retail", "name=Торговый центр Пятница")));
        assertEquals("{category_shopping}", categoryOf("building=retail", "name=Торговый центр Пятница"));
        assertEquals(List.of("shop-mall"), classifier.poiTypes(tags("building=retail", "shop=mall", "name=Mall")));
        assertEquals(List.of(), classifier.poiTypes(tags("building=retail")));
        assertEquals(List.of("natural-tree"), classifier.poiTypes(tags("natural=tree", "name=Карамзинский дуб")));
        assertEquals("{category_tourism}", categoryOf("natural=tree", "denotation=natural_monument", "name=Kaisereiche"));
        assertEquals(List.of(), classifier.poiTypes(tags("natural=tree", "denotation=natural_monument")));
        for (String species : List.of("Eiche", "Stieleiche", "Stieleiche - Quercus robur", "Kirschbaum", "Ginkgo", "Palm Tree", "Дуб", "Дуб черешчатый")) {
            assertEquals(List.of(), classifier.poiTypes(tags("natural=tree", "name=" + species)), species);
        }
        assertEquals(List.of("natural-tree"), classifier.poiTypes(tags("natural=tree", "name=Kaisereiche")));
        assertEquals(List.of("natural-tree"), classifier.poiTypes(tags("natural=tree", "name=Дуб «Космос»")));
    }

    @Test
    void poiTypes_rescueSeaAndSynonymTags() {
        assertEquals(List.of("emergency-lifeguard"), classifier.poiTypes(tags("emergency=water_rescue", "name=DLRG")));
        assertEquals(List.of("emergency-lifeguard"), classifier.poiTypes(tags("emergency=lifeguard_base")));
        assertEquals(List.of("emergency-rescue_station"), classifier.poiTypes(tags("amenity=rescue_station", "name=ПСО")));
        assertEquals(List.of("emergency-rescue_station"), classifier.poiTypes(tags("emergency=disaster_response", "name=THW")));
        assertEquals("{category_rescue}", categoryOf("amenity=rescue_station"));
        assertEquals(List.of("waterway-boatyard"), classifier.poiTypes(tags("waterway=boatyard", "name=Werft")));
        assertEquals("{category_water_activities}", categoryOf("waterway=boatyard"));
        assertEquals(List.of("waterway-fuel"), classifier.poiTypes(tags("waterway=fuel", "name=Bootstankstelle")));
        assertEquals("{category_water_activities}", categoryOf("waterway=fuel"));
        assertEquals(List.of("tourism-chalet"), classifier.poiTypes(tags("tourism=cabin", "name=Cristaux")));
        assertEquals(List.of("amenity-community_centre"), classifier.poiTypes(tags("amenity=social_centre", "name=Kieztreff")));
        assertEquals(List.of("amenity-events_venue"), classifier.poiTypes(tags("amenity=events_centre", "name=Kraftwerk Berlin")));
        assertEquals(List.of("office-translator"), classifier.poiTypes(tags("amenity=translator", "name=Бюро переводов")));
        assertEquals(List.of("shop-beauty"), classifier.poiTypes(tags("leisure=tanning_salon", "name=SunBox")));
        assertEquals(List.of("leisure-swimming_area"), classifier.poiTypes(tags("leisure=bathing_place", "name=Strandbad")));
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
