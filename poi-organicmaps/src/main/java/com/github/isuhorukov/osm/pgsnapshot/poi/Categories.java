package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

final class Categories {

    static final String DISPLAYED_CATEGORY_PREFIX = "category_";

    private final Map<String, Set<String>> categoriesByType;

    private Categories(Map<String, Set<String>> categoriesByType) {
        this.categoriesByType = categoriesByType;
    }

    static Categories parse(BufferedReader reader) throws IOException {
        Map<String, Set<String>> categoriesByType = new HashMap<>();
        boolean header = true;
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                header = true;
            } else if (header && !line.startsWith("#")) {
                addEntry(line.trim(), categoriesByType);
                header = false;
            }
        }
        categoriesByType.replaceAll((type, categories) -> Collections.unmodifiableSet(categories));
        return new Categories(categoriesByType);
    }

    private static void addEntry(String header, Map<String, Set<String>> categoriesByType) {
        List<String> types = new ArrayList<>();
        Set<String> categories = new TreeSet<>();
        for (String token : Tokens.split(header, "|")) {
            if (!token.startsWith("@")) {
                types.add(token);
            } else if (token.startsWith("@" + DISPLAYED_CATEGORY_PREFIX)) {
                categories.add(token.substring(1));
            }
        }
        for (String type : types) {
            categoriesByType.computeIfAbsent(type, key -> new TreeSet<>()).addAll(categories);
        }
    }

    Optional<Set<String>> categoriesOf(List<String> type) {
        for (int length = type.size(); length >= Math.min(2, type.size()); length--) {
            Set<String> categories = categoriesByType.get(MapcssMapping.typeName(type.subList(0, length)));
            if (categories != null) {
                return Optional.of(categories);
            }
        }
        return Optional.empty();
    }
}
