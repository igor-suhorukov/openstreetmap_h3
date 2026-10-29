package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdminObjectFilterTest {

    private static List<Tag> tags(String pairs) {
        List<Tag> tags = new ArrayList<>();
        if (pairs == null) {
            return tags;
        }
        for (String pair : pairs.split(";")) {
            String[] keyValue = pair.split("=", 2);
            tags.add(new Tag(keyValue[0], keyValue[1]));
        }
        return tags;
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "building=yes|true",
            "is.poi=amenity-cafe|true",
            "addr:housenumber=12|true",
            "place=village|true",
            "public_transport=platform|true",
            "public_transport=stop_position|true",
            "public_transport=station|false",
            "railway=platform|true",
            "railway=train_station_entrance|true",
            "railway=rail|false",
            "highway=platform|true",
            "highway=residential|false",
            "highway=traffic_signals|false",
            "amenity=parking|true",
            "amenity=toilets|true",
            "amenity=atm|true",
            "amenity=fuel|true",
            "amenity=bench|false",
            "boundary=national_park|true",
            "boundary=protected_area|true",
            "leisure=nature_reserve|true",
            "leisure=park|false",
            "boundary=administrative;is.poi=x|false",
            "boundary=land_area;place=island|false",
            "waterway=river;is.poi=x|false",
            "waterway=riverbank;building=yes|false",
            "natural=water;water=river;is.poi=x|false",
            "natural=water;water=lake;is.poi=x|true",
            "natural=water;water=river|false",
            "waterway=stream;is.poi=x|true",
            "name=x|false",
            "|false"
    })
    void isBound_followsDecisions28And38(String pairs, boolean expected) {
        assertEquals(expected, AdminObjectFilter.isBound(tags(pairs)));
    }
}
