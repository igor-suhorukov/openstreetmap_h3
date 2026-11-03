package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

final class PoiRules {

    private final Map<String, Set<String>> included;
    private final Map<String, Set<String>> removed;
    private final Map<String, Map<String, Set<String>>> tagConditions;
    private final Set<String> excluded;
    private final Map<String, List<Map<String, Set<String>>>> excludedWhen;

    private PoiRules(Map<String, Set<String>> included, Map<String, Set<String>> removed, Map<String, Map<String, Set<String>>> tagConditions,
                     Set<String> excluded, Map<String, List<Map<String, Set<String>>>> excludedWhen) {
        this.included = included;
        this.removed = removed;
        this.tagConditions = tagConditions;
        this.excluded = excluded;
        this.excludedWhen = excludedWhen;
    }

    static PoiRules parse(BufferedReader reader) throws IOException {
        Map<String, Set<String>> included = new HashMap<>();
        Map<String, Set<String>> removed = new HashMap<>();
        Map<String, Map<String, Set<String>>> tagConditions = new HashMap<>();
        Set<String> excluded = new HashSet<>();
        Map<String, List<Map<String, Set<String>>>> excludedWhen = new HashMap<>();
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
            List<String> arguments = tokens.subList(1, tokens.size());
            if (rule.charAt(0) == '+' && !type.isEmpty()) {
                included.put(type, categories(arguments));
                removed.put(type, removedCategories(arguments));
                tagConditions.put(type, conditions(arguments, lineNumber, line));
            } else if (rule.charAt(0) == '-' && !type.isEmpty() && arguments.isEmpty()) {
                excluded.add(type);
            } else if (rule.charAt(0) == '-' && !type.isEmpty() && categories(arguments).isEmpty() && removedCategories(arguments).isEmpty()) {
                excludedWhen.computeIfAbsent(type, key -> new ArrayList<>()).add(conditions(arguments, lineNumber, line));
            } else {
                throw new IllegalArgumentException("Expected '+type [category...]', '-type' or '-type key=value' in line " + lineNumber + ": " + line);
            }
        }
        return new PoiRules(included, removed, tagConditions, excluded, excludedWhen);
    }

    private static Set<String> categories(List<String> arguments) {
        Set<String> categories = new TreeSet<>();
        for (String argument : arguments) {
            if (argument.indexOf('=') < 0 && argument.charAt(0) != '!') {
                categories.add(argument);
            }
        }
        return Collections.unmodifiableSet(categories);
    }

    private static Set<String> removedCategories(List<String> arguments) {
        Set<String> categories = new TreeSet<>();
        for (String argument : arguments) {
            if (argument.indexOf('=') < 0 && argument.charAt(0) == '!' && argument.length() > 1) {
                categories.add(argument.substring(1));
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

    Optional<String> exclusionRule(String type) {
        return isExcluded(type) ? mostSpecificRule(excluded, type) : Optional.empty();
    }

    Optional<String> inclusionRule(String type) {
        return mostSpecificRule(included.keySet(), type);
    }

    List<Map<String, Set<String>>> exclusionConditions(String type) {
        List<Map<String, Set<String>>> conditions = new ArrayList<>();
        excludedWhen.forEach((rule, ruleConditions) -> {
            if (appliesTo(rule, type)) {
                conditions.addAll(ruleConditions);
            }
        });
        return conditions;
    }

    Optional<Set<String>> includedCategories(String type) {
        return mostSpecificRule(included.keySet(), type).map(included::get);
    }

    Set<String> removedCategories(String type) {
        return mostSpecificRule(included.keySet(), type).map(removed::get).orElse(Set.of());
    }

    Map<String, Set<String>> tagConditions(String type) {
        return mostSpecificRule(included.keySet(), type).map(tagConditions::get).orElse(Map.of());
    }

    Set<String> ruleTypes() {
        Set<String> types = new TreeSet<>(included.keySet());
        types.addAll(excluded);
        types.addAll(excludedWhen.keySet());
        return types;
    }

    static boolean appliesTo(String rule, String type) {
        return type.equals(rule) || type.startsWith(rule + '-');
    }

    private static Optional<String> mostSpecificRule(Collection<String> rules, String type) {
        return rules.stream().filter(rule -> appliesTo(rule, type)).max(Comparator.comparingInt(String::length));
    }
}
