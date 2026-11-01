package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

final class PoiRules {

    private final Map<String, Set<String>> included;
    private final Map<String, Map<String, Set<String>>> tagConditions;
    private final Set<String> excluded;

    private PoiRules(Map<String, Set<String>> included, Map<String, Map<String, Set<String>>> tagConditions, Set<String> excluded) {
        this.included = included;
        this.tagConditions = tagConditions;
        this.excluded = excluded;
    }

    static PoiRules parse(BufferedReader reader) throws IOException {
        Map<String, Set<String>> included = new HashMap<>();
        Map<String, Map<String, Set<String>>> tagConditions = new HashMap<>();
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
                List<String> arguments = tokens.subList(1, tokens.size());
                included.put(type, categories(arguments));
                tagConditions.put(type, conditions(arguments, lineNumber, line));
            } else if (rule.charAt(0) == '-' && !type.isEmpty() && tokens.size() == 1) {
                excluded.add(type);
            } else {
                throw new IllegalArgumentException("Expected '+type [category...]' or '-type' in line " + lineNumber + ": " + line);
            }
        }
        return new PoiRules(included, tagConditions, excluded);
    }

    private static Set<String> categories(List<String> arguments) {
        Set<String> categories = new TreeSet<>();
        for (String argument : arguments) {
            if (argument.indexOf('=') < 0) {
                categories.add(argument);
            }
        }
        return Collections.unmodifiableSet(categories);
    }

    private static Map<String, Set<String>> conditions(List<String> arguments, int lineNumber, String line) {
        Map<String, Set<String>> conditions = new HashMap<>();
        for (String argument : arguments) {
            int separator = argument.indexOf('=');
            if (separator >= 0) {
                Set<String> values = Set.copyOf(Tokens.split(argument.substring(separator + 1), ";"));
                if (separator == 0 || values.isEmpty()) {
                    throw new IllegalArgumentException("Expected 'key=value;value' in line " + lineNumber + ": " + line);
                }
                conditions.put(argument.substring(0, separator), values);
            }
        }
        return Collections.unmodifiableMap(conditions);
    }

    boolean isExcluded(String type) {
        Optional<String> exclusion = mostSpecificRule(excluded, type);
        Optional<String> inclusion = mostSpecificRule(included.keySet(), type);
        return exclusion.isPresent() && (inclusion.isEmpty() || inclusion.get().length() <= exclusion.get().length());
    }

    Optional<Set<String>> includedCategories(String type) {
        return mostSpecificRule(included.keySet(), type).map(included::get);
    }

    Map<String, Set<String>> tagConditions(String type) {
        return mostSpecificRule(included.keySet(), type).map(tagConditions::get).orElse(Map.of());
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
