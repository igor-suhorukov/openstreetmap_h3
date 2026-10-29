package com.github.isuhorukov.osm.pgsnapshot.admin;

import org.locationtech.jts.io.WKBReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

final class AdminSource {

    static final String ADMINISTRATIVE = "administrative";
    private static final int BUFFER_SIZE = 1 << 20;
    private static final String RELATION = "relation";
    private static final String SUBAREA = "subarea";

    private final List<AdminUnit> units;
    private final Map<Long, Integer> byId;

    private AdminSource(List<AdminUnit> units, Map<Long, Integer> byId) {
        this.units = units;
        this.byId = byId;
    }

    List<AdminUnit> getUnits() {
        return units;
    }

    Map<Long, Integer> getById() {
        return byId;
    }

    static AdminSource readRelations(InputStream opl, MessageDigest digest) throws IOException {
        List<Relation> relations = parseRelations(opl, digest);
        List<AdminUnit> units = new ArrayList<>();
        Map<Long, Integer> byId = new HashMap<>();
        for (Relation relation : relations) {
            Integer level = parseLevel(relation.tags.get("admin_level"));
            if (ADMINISTRATIVE.equals(relation.tags.get("boundary")) && level != null && !byId.containsKey(relation.id)) {
                byId.put(relation.id, units.size());
                units.add(new AdminUnit(units.size(), relation.id, level, relation.tags));
            }
        }
        for (Relation relation : relations) {
            Integer parent = byId.get(relation.id);
            if (parent != null) {
                linkSubareas(relation, parent, units, byId);
            }
        }
        return new AdminSource(Collections.unmodifiableList(units), byId);
    }

    private static List<Relation> parseRelations(InputStream opl, MessageDigest digest) throws IOException {
        List<Relation> relations = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new DigestInputStream(opl, digest), StandardCharsets.UTF_8), BUFFER_SIZE)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Relation relation = Relation.parse(line);
                if (relation != null) {
                    relations.add(relation);
                }
            }
        }
        relations.sort(Comparator.comparingLong(relation -> relation.id));
        return relations;
    }

    private static void linkSubareas(Relation relation, int parent, List<AdminUnit> units, Map<Long, Integer> byId) {
        for (int i = 0; i < relation.memberTypes.size(); i++) {
            Integer child = relation.memberTypes.get(i) == 'r' && SUBAREA.equals(relation.memberRoles.get(i))
                    ? byId.get(relation.memberIds.get(i)) : null;
            if (child != null && units.get(child).getSubareaParent() < 0) {
                units.get(child).setSubareaParent(parent);
            }
        }
    }

    void readPolygons(InputStream polygons, MessageDigest digest) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new DigestInputStream(polygons, digest), StandardCharsets.UTF_8), BUFFER_SIZE)) {
            String line;
            while ((line = reader.readLine()) != null) {
                int firstTab = line.indexOf('\t');
                int secondTab = firstTab < 0 ? -1 : line.indexOf('\t', firstTab + 1);
                if (secondTab < 0 || !RELATION.equals(line.substring(firstTab + 1, secondTab))) {
                    continue;
                }
                int thirdTab = line.indexOf('\t', secondTab + 1);
                long id = Long.parseLong(line.substring(secondTab + 1, thirdTab < 0 ? line.length() : thirdTab));
                Integer index = byId.get(id);
                if (index != null) {
                    units.get(index).setEwkb(WKBReader.hexToBytes(line.substring(0, firstTab)));
                }
            }
        }
    }

    static Integer parseLevel(String value) {
        if (value == null || value.isEmpty() || value.length() > 2) {
            return null;
        }
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return null;
            }
        }
        return Integer.parseInt(value);
    }

    static String oplUnescape(String value) {
        if (value.indexOf('%') < 0) {
            return value;
        }
        StringBuilder result = new StringBuilder(value.length());
        int i = 0;
        while (i < value.length()) {
            char c = value.charAt(i);
            int end = c == '%' ? value.indexOf('%', i + 1) : -1;
            if (end > i) {
                result.appendCodePoint(Integer.parseInt(value.substring(i + 1, end), 16));
                i = end + 1;
            } else {
                result.append(c);
                i++;
            }
        }
        return result.toString();
    }

    private static final class Relation {
        private final long id;
        private final SortedMap<String, String> tags;
        private final List<Character> memberTypes = new ArrayList<>();
        private final List<Long> memberIds = new ArrayList<>();
        private final List<String> memberRoles = new ArrayList<>();

        private Relation(long id, SortedMap<String, String> tags) {
            this.id = id;
            this.tags = tags;
        }

        private static Relation parse(String line) {
            if (line.isEmpty() || line.charAt(0) != 'r') {
                return null;
            }
            String[] tokens = line.split(" ");
            Relation relation = new Relation(Long.parseLong(tokens[0].substring(1)), new TreeMap<>());
            for (int i = 1; i < tokens.length; i++) {
                String token = tokens[i];
                if (token.length() > 1 && token.charAt(0) == 'T') {
                    relation.parseTags(token.substring(1));
                } else if (token.length() > 1 && token.charAt(0) == 'M') {
                    relation.parseMembers(token.substring(1));
                }
            }
            return relation;
        }

        private void parseTags(String field) {
            for (String pair : field.split(",")) {
                int equals = pair.indexOf('=');
                if (equals > 0) {
                    tags.put(oplUnescape(pair.substring(0, equals)), oplUnescape(pair.substring(equals + 1)));
                }
            }
        }

        private void parseMembers(String field) {
            for (String member : field.split(",")) {
                int at = member.indexOf('@');
                if (at > 1) {
                    memberTypes.add(member.charAt(0));
                    memberIds.add(Long.parseLong(member.substring(1, at)));
                    memberRoles.add(oplUnescape(member.substring(at + 1)));
                }
            }
        }
    }
}
