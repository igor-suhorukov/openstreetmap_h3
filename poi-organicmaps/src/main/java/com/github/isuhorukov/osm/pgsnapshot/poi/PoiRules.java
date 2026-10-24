package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

final class PoiRules {

    private final Map<String, Set<String>> included;
    private final Set<String> excluded;

    private PoiRules(Map<String, Set<String>> included, Set<String> excluded) {
        this.included = included;
        this.excluded = excluded;
    }

    static PoiRules parse(BufferedReader reader) throws IOException {
        Map<String, Set<String>> included = new HashMap<>();
        Set<String> excluded = new HashSet<>();
        String line;
        int lineNumber = 0;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            line = line.trim();
            if (line.isEmpty() || line.charAt(0) == '#') {
                continue;
            }
            List<String> tokens = Tokens.split(line, " \t");
            String rule = tokens.get(0);
            String type = rule.substring(1);
            if (rule.charAt(0) == '+' && !type.isEmpty()) {
                included.put(type, Collections.unmodifiableSet(new TreeSet<>(tokens.subList(1, tokens.size()))));
            } else if (rule.charAt(0) == '-' && !type.isEmpty() && tokens.size() == 1) {
                excluded.add(type);
            } else {
                throw new IllegalArgumentException("Expected '+type [category...]' or '-type' in line " + lineNumber + ": " + line);
            }
        }
        return new PoiRules(included, excluded);
    }

    boolean isExcluded(String type) {
        return mostSpecificRule(excluded, type).isPresent();
    }

    Optional<Set<String>> includedCategories(String type) {
        return mostSpecificRule(included.keySet(), type).map(included::get);
    }

    Set<String> ruleTypes() {
        Set<String> types = new TreeSet<>(included.keySet());
        types.addAll(excluded);
        return types;
    }

    static boolean appliesTo(String rule, String type) {
        return type.equals(rule) || type.startsWith(rule + '-');
    }

    private static Optional<String> mostSpecificRule(Collection<String> rules, String type) {
        return rules.stream().filter(rule -> appliesTo(rule, type)).max(Comparator.comparingInt(String::length));
    }
}
