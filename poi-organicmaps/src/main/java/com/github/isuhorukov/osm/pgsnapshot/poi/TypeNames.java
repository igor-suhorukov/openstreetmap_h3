package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

final class TypeNames {

    static final TypeNames EMPTY = new TypeNames(Collections.emptyMap());

    private static final String TYPE_SECTION_PREFIX = "[type.";

    private final Map<String, Map<String, String>> namesByType;

    private TypeNames(Map<String, Map<String, String>> namesByType) {
        this.namesByType = namesByType;
    }

    static TypeNames parse(BufferedReader reader, Collection<String> languages) throws IOException {
        Map<String, Map<String, String>> namesByType = new HashMap<>();
        Map<String, String> names = null;
        String line;
        while ((line = reader.readLine()) != null) {
            String text = line.trim();
            if (text.startsWith("[")) {
                names = null;
                if (text.startsWith(TYPE_SECTION_PREFIX) && text.endsWith("]")) {
                    String type = text.substring(TYPE_SECTION_PREFIX.length(), text.length() - 1).replace('.', '-');
                    names = namesByType.computeIfAbsent(type, key -> new HashMap<>());
                }
            } else if (names != null) {
                int separator = text.indexOf('=');
                if (separator > 0) {
                    String language = text.substring(0, separator).trim();
                    if (languages.contains(language)) {
                        names.put(language, text.substring(separator + 1).trim());
                    }
                }
            }
        }
        return new TypeNames(namesByType);
    }

    String name(List<String> type, String language) {
        for (int length = type.size(); length >= Math.min(2, type.size()); length--) {
            Map<String, String> names = namesByType.get(MapcssMapping.typeName(type.subList(0, length)));
            if (names != null && names.containsKey(language)) {
                return names.get(language);
            }
        }
        return null;
    }
}
