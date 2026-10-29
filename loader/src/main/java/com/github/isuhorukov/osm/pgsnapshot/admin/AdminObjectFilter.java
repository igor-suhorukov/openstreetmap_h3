package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Set;

public final class AdminObjectFilter {

    public static final String POI_TAG = "is.poi";
    private static final String PLATFORM = "platform";
    private static final Set<String> UNIT_BOUNDARIES = Set.of(AdminSource.ADMINISTRATIVE, "land_area");
    private static final Set<String> PROTECTED_BOUNDARIES = Set.of("national_park", "protected_area");
    private static final Set<String> TRANSPORT_PLATFORMS = Set.of(PLATFORM, "stop_position");
    private static final Set<String> RAILWAY_STOPS = Set.of(PLATFORM, "train_station_entrance");
    private static final Set<String> AMENITIES = Set.of("parking", "toilets", "atm", "fuel");
    private static final Set<String> RIVERS = Set.of("river", "riverbank");

    private AdminObjectFilter() {
    }

    public static boolean isBound(Collection<Tag> tags) {
        boolean listed = false;
        boolean naturalWater = false;
        boolean riverWater = false;
        for (Tag tag : tags) {
            String value = tag.getValue();
            switch (tag.getKey()) {
                case "boundary":
                    if (UNIT_BOUNDARIES.contains(value)) {
                        return false;
                    }
                    listed |= PROTECTED_BOUNDARIES.contains(value);
                    break;
                case "waterway":
                    if (RIVERS.contains(value)) {
                        return false;
                    }
                    break;
                case "natural":
                    naturalWater = "water".equals(value);
                    break;
                case "water":
                    riverWater = "river".equals(value);
                    break;
                case "building":
                case POI_TAG:
                case "addr:housenumber":
                case "place":
                    listed = true;
                    break;
                case "public_transport":
                    listed |= TRANSPORT_PLATFORMS.contains(value);
                    break;
                case "railway":
                    listed |= RAILWAY_STOPS.contains(value);
                    break;
                case "highway":
                    listed |= PLATFORM.equals(value);
                    break;
                case "amenity":
                    listed |= AMENITIES.contains(value);
                    break;
                case "leisure":
                    listed |= "nature_reserve".equals(value);
                    break;
                default:
                    break;
            }
        }
        return listed && !(naturalWater && riverWater);
    }
}
