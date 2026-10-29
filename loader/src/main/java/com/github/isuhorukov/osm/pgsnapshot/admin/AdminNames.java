package com.github.isuhorukov.osm.pgsnapshot.admin;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

final class AdminNames {

    private static final String NAME = "name";
    private static final String NAME_PREFIX = "name:";

    private AdminNames() {
    }

    static String name(Map<String, String> tags, List<String> nameLang) {
        for (String language : nameLang) {
            String value = tags.get(NAME.equals(language) ? NAME : NAME_PREFIX + language);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        for (String key : new String[]{NAME, "int_name"}) {
            String value = tags.get(key);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        for (Map.Entry<String, String> tag : new TreeMap<>(tags).entrySet()) {
            if (tag.getKey().startsWith(NAME_PREFIX) && !tag.getValue().isEmpty()) {
                return tag.getValue();
            }
        }
        return null;
    }

    static SortedMap<String, String> names(Map<String, String> tags) {
        SortedMap<String, String> names = new TreeMap<>();
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            String key = tag.getKey();
            if (key.equals(NAME) || key.startsWith(NAME_PREFIX) || key.endsWith("_name") || key.contains("_name:")) {
                names.put(key, tag.getValue());
            }
        }
        return names;
    }
}
