package com.github.isuhorukov.osm.pgsnapshot.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

final class TagReplacer {

    private static final class Replacement {
        private final List<MutableTag> tags;
        private final boolean update;

        private Replacement(List<MutableTag> tags, boolean update) {
            this.tags = tags;
            this.update = update;
        }
    }

    private final Map<String, Map<String, Replacement>> replacements;

    private TagReplacer(Map<String, Map<String, Replacement>> replacements) {
        this.replacements = replacements;
    }

    static TagReplacer parse(BufferedReader reader) throws IOException {
        Map<String, Map<String, Replacement>> replacements = new HashMap<>();
        String line;
        int lineNumber = 0;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            line = line.trim();
            if (!line.isEmpty() && line.charAt(0) != '#') {
                addRule(line, lineNumber, replacements);
            }
        }
        return new TagReplacer(replacements);
    }

    private static void addRule(String line, int lineNumber, Map<String, Map<String, Replacement>> replacements) {
        int keyEnd = line.indexOf('=');
        int valueEnd = keyEnd < 0 ? -1 : line.indexOf(':', keyEnd + 1);
        if (valueEnd < 0) {
            throw new IllegalArgumentException("Cannot find source tag in line " + lineNumber + ": " + line);
        }
        String key = line.substring(0, keyEnd).trim();
        String value = line.substring(keyEnd + 1, valueEnd).trim();
        int flagStart = line.indexOf('|', valueEnd + 1);
        String replacementList = line.substring(valueEnd + 1, flagStart < 0 ? line.length() : flagStart);
        Replacement replacement = new Replacement(parseTags(replacementList, lineNumber), flagStart >= 0);
        if (replacements.computeIfAbsent(key, k -> new HashMap<>()).put(value, replacement) != null) {
            throw new IllegalArgumentException("Duplicate rule for " + key + '=' + value + " in line " + lineNumber);
        }
    }

    private static List<MutableTag> parseTags(String replacementList, int lineNumber) {
        List<MutableTag> tags = new ArrayList<>();
        for (String token : Tokens.split(replacementList, ",")) {
            List<String> keyValue = Tokens.split(token, "=");
            if (keyValue.size() != 2) {
                throw new IllegalArgumentException("Cannot parse replacement tag '" + token + "' in line " + lineNumber);
            }
            tags.add(new MutableTag(keyValue.get(0).trim(), keyValue.get(1).trim()));
        }
        return tags;
    }

    boolean replaces(String key, String value) {
        Map<String, Replacement> byValue = replacements.get(key);
        return byValue != null && byValue.containsKey(value);
    }

    void apply(List<MutableTag> tags) {
        for (Replacement replacement : removeReplacedTags(tags)) {
            for (MutableTag tag : replacement.tags) {
                if (replacement.update) {
                    update(tags, tag.key, tag.value);
                } else {
                    add(tags, tag.key, tag.value);
                }
            }
        }
    }

    private List<Replacement> removeReplacedTags(List<MutableTag> tags) {
        List<Replacement> applied = Collections.emptyList();
        for (Iterator<MutableTag> iterator = tags.iterator(); iterator.hasNext(); ) {
            MutableTag tag = iterator.next();
            Map<String, Replacement> byValue = replacements.get(tag.key);
            Replacement replacement = byValue != null ? byValue.get(tag.value) : null;
            if (replacement != null) {
                if (applied.isEmpty()) {
                    applied = new ArrayList<>(2);
                }
                applied.add(replacement);
                iterator.remove();
            }
        }
        return applied;
    }

    private static void update(List<MutableTag> tags, String key, String value) {
        for (MutableTag tag : tags) {
            if (tag.key.equals(key)) {
                tag.value = value;
                return;
            }
        }
        add(tags, key, value);
    }

    private static void add(List<MutableTag> tags, String key, String value) {
        if (!key.isEmpty() && !value.isEmpty()) {
            tags.add(new MutableTag(key, value));
        }
    }
}
