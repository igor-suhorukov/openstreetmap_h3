package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.util.List;
import java.util.Map;

public final class PoiType {
    private final String type;
    private final List<String> categories;
    private final Map<String, String> names;

    PoiType(String type, List<String> categories, Map<String, String> names) {
        this.type = type;
        this.categories = List.copyOf(categories);
        this.names = Map.copyOf(names);
    }

    public String getType() {
        return type;
    }

    public List<String> getCategories() {
        return categories;
    }

    public String getName(String language) {
        return names.get(language);
    }
}
