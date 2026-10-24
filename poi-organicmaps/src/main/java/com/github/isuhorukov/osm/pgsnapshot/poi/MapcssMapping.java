package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

final class MapcssMapping {

    static final class Rule {
        final List<String> type;
        private final String[] keys;
        private final String[] values;
        private final String[] mandatoryKeys;
        private final String[] forbiddenKeys;

        Rule(List<String> type, List<String> keys, List<String> values,
             List<String> mandatoryKeys, List<String> forbiddenKeys) {
            this.type = type;
            this.keys = keys.toArray(new String[0]);
            this.values = values.toArray(new String[0]);
            this.mandatoryKeys = mandatoryKeys.toArray(new String[0]);
            this.forbiddenKeys = forbiddenKeys.toArray(new String[0]);
        }

        boolean matches(List<MutableTag> tags) {
            for (int i = 0; i < keys.length; i++) {
                if (!hasTag(tags, keys[i], values[i])) {
                    return false;
                }
            }
            for (String key : mandatoryKeys) {
                if (!hasValueOtherThanNo(tags, key)) {
                    return false;
                }
            }
            for (String key : forbiddenKeys) {
                if (hasValueOtherThanNo(tags, key)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean hasTag(List<MutableTag> tags, String key, String value) {
            for (MutableTag tag : tags) {
                if (tag.key.equals(key) && tag.value.equals(value)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean hasValueOtherThanNo(List<MutableTag> tags, String key) {
            for (MutableTag tag : tags) {
                if (tag.key.equals(key) && !"no".equals(tag.value)) {
                    return true;
                }
            }
            return false;
        }
    }

    private final Map<String, Map<String, List<Rule>>> rulesByTag = new HashMap<>();
    private final Map<String, List<Rule>> rulesByMandatoryKey = new HashMap<>();
    private final List<Rule> unindexedRules = new ArrayList<>();

    MapcssMapping(Collection<Rule> rules) {
        for (Rule rule : rules) {
            if (rule.keys.length > 0) {
                rulesByTag.computeIfAbsent(rule.keys[0], key -> new HashMap<>())
                        .computeIfAbsent(rule.values[0], value -> new ArrayList<>()).add(rule);
            } else if (rule.mandatoryKeys.length > 0) {
                rulesByMandatoryKey.computeIfAbsent(rule.mandatoryKeys[0], key -> new ArrayList<>()).add(rule);
            } else {
                unindexedRules.add(rule);
            }
        }
    }

    static List<Rule> parseRules(BufferedReader reader) throws IOException {
        List<Rule> rules = new ArrayList<>();
        String line;
        int lineNumber = 0;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            if (!line.isEmpty() && line.charAt(0) != '#') {
                addRules(line, lineNumber, rules);
            }
        }
        return rules;
    }

    private static void addRules(String line, int lineNumber, List<Rule> rules) {
        String[] fields = line.split(";", -1);
        for (int i = 0; i < fields.length; i++) {
            fields[i] = fields[i].trim();
        }
        if (fields.length != 3 && fields.length != 7) {
            throw new IllegalArgumentException("Expected 3 or 7 fields in line " + lineNumber + ": " + line);
        }
        List<String> type = List.copyOf(Tokens.split(fields[0], "|"));
        if (fields.length == 3 && fields[2].isEmpty()) {
            if (type.size() != 2) {
                throw new IllegalArgumentException("Short format requires a two-level type in line " + lineNumber + ": " + line);
            }
            rules.add(new Rule(type, List.of(type.get(0)), List.of(type.get(1)), List.of(), List.of()));
        } else if (fields.length == 7 && !"x".equals(fields[2])) {
            for (String selector : Tokens.split(fields[1], ",")) {
                rules.add(parseSelector(type, selector, lineNumber));
            }
        }
    }

    private static Rule parseSelector(List<String> type, String selector, int lineNumber) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        List<String> mandatoryKeys = new ArrayList<>();
        List<String> forbiddenKeys = new ArrayList<>();
        for (String condition : Tokens.split(selector.trim(), "[]")) {
            List<String> keyValue = Tokens.split(condition, "=");
            if (keyValue.size() == 1) {
                String key = Tokens.strip(keyValue.get(0), "?!");
                if (keyValue.get(0).charAt(0) == '!') {
                    forbiddenKeys.add(key);
                } else {
                    mandatoryKeys.add(key);
                }
            } else if (keyValue.size() == 2) {
                keys.add(keyValue.get(0));
                values.add(keyValue.get(1));
            } else {
                throw new IllegalArgumentException("Cannot parse selector " + selector + " in line " + lineNumber);
            }
        }
        return new Rule(type, keys, values, mandatoryKeys, forbiddenKeys);
    }

    boolean isIndexed(String key, String value) {
        Map<String, List<Rule>> byValue = rulesByTag.get(key);
        return (byValue != null && byValue.containsKey(value)) || rulesByMandatoryKey.containsKey(key);
    }

    boolean hasUnindexedRules() {
        return !unindexedRules.isEmpty();
    }

    List<List<String>> match(List<MutableTag> tags) {
        Set<List<String>> matched = new HashSet<>();
        for (MutableTag tag : tags) {
            Map<String, List<Rule>> byValue = rulesByTag.get(tag.key);
            if (byValue != null) {
                addMatched(byValue.get(tag.value), tags, matched);
            }
            addMatched(rulesByMandatoryKey.get(tag.key), tags, matched);
        }
        addMatched(unindexedRules, tags, matched);
        return leaveLongestTypes(matched);
    }

    private static void addMatched(List<Rule> rules, List<MutableTag> tags, Set<List<String>> matched) {
        if (rules == null) {
            return;
        }
        for (Rule rule : rules) {
            if (rule.matches(tags)) {
                matched.add(rule.type);
            }
        }
    }

    static List<List<String>> leaveLongestTypes(Collection<List<String>> types) {
        List<List<String>> longest = new ArrayList<>(types.size());
        for (List<String> type : types) {
            boolean hasLonger = false;
            for (List<String> other : types) {
                if (other.size() > type.size() && equalPrefix(type, other)) {
                    hasLonger = true;
                    break;
                }
            }
            if (!hasLonger) {
                longest.add(type);
            }
        }
        longest.sort(Comparator.comparing(MapcssMapping::typeName));
        return longest;
    }

    private static boolean equalPrefix(List<String> first, List<String> second) {
        int length = Math.min(2, Math.min(first.size(), second.size()));
        return first.subList(0, length).equals(second.subList(0, length));
    }

    static String typeName(List<String> type) {
        return String.join("-", type);
    }
}
