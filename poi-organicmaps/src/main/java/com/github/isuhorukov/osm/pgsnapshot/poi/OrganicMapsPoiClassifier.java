package com.github.isuhorukov.osm.pgsnapshot.poi;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public final class OrganicMapsPoiClassifier {

    private static final Logger log = LoggerFactory.getLogger(OrganicMapsPoiClassifier.class);

    public static final String POI_TAG = "is.poi";
    public static final String POI_CATEGORY_TAG = "is.poi_category";
    public static final List<String> NAME_LANGUAGES = List.of("en", "ru", "th");

    static final String MAPPING_FILE = "mapcss-mapping.csv";
    static final String REPLACED_TAGS_FILE = "replaced_tags.txt";
    static final String CATEGORIES_FILE = "categories.txt";
    static final String TYPE_NAMES_FILE = "strings/types_strings.txt";
    static final String RULES_RESOURCE = "organicmaps_poi_rules.txt";

    private static final Set<String> ONE_LEVEL_POI_CLASSES = Set.of("amenity", "craft", "emergency", "healthcare",
            "historic", "leisure", "mountain_pass", "office", "railway", "shop", "sport", "tourism");
    private static final Set<String> TWO_LEVEL_POI_TYPES = Set.of("aeroway-terminal", "aeroway-gate",
            "building-guardhouse", "building-train_station", "highway-bus_stop", "highway-elevator", "highway-ford",
            "highway-raceway", "highway-rest_area", "highway-services", "highway-speed_camera", "man_made-cross",
            "man_made-lighthouse", "man_made-water_tap", "man_made-water_well", "man_made-windmill", "natural-beach",
            "natural-cave_entrance", "natural-geyser", "natural-hot_spring", "natural-peak", "natural-saddle",
            "natural-spring", "natural-volcano", "waterway-waterfall");

    private final TagReplacer tagReplacer;
    private final MapcssMapping mapping;
    private final Map<List<String>, Set<String>> poiCategories;
    private final List<PoiType> poiTypes;
    private final Set<String> unknownRuleTypes;

    OrganicMapsPoiClassifier(TagReplacer tagReplacer, List<MapcssMapping.Rule> rules, Categories categories,
                             TypeNames typeNames, PoiRules poiRules) {
        this.tagReplacer = tagReplacer;
        Set<List<String>> types = new LinkedHashSet<>();
        for (MapcssMapping.Rule rule : rules) {
            types.add(rule.type);
        }
        this.unknownRuleTypes = unknownRuleTypes(types, poiRules);
        this.mapping = new MapcssMapping(candidateRules(rules, includedGroups(types, poiRules)));
        Map<List<String>, Set<String>> categoriesByType = new HashMap<>();
        for (List<String> type : types) {
            poiCategories(type, categories, poiRules).ifPresent(typeCategories -> categoriesByType.put(type, typeCategories));
        }
        this.poiCategories = categoriesByType;
        this.poiTypes = catalog(types, categoriesByType, typeNames);
    }

    private static Set<String> unknownRuleTypes(Set<List<String>> types, PoiRules poiRules) {
        Set<String> unknown = new TreeSet<>(poiRules.ruleTypes());
        for (List<String> type : types) {
            String name = MapcssMapping.typeName(type);
            unknown.removeIf(rule -> PoiRules.appliesTo(rule, name));
        }
        return Collections.unmodifiableSet(unknown);
    }

    private static Set<List<String>> includedGroups(Set<List<String>> types, PoiRules poiRules) {
        Set<List<String>> groups = new HashSet<>();
        for (List<String> type : types) {
            if (poiRules.includedCategories(MapcssMapping.typeName(type)).isPresent()) {
                groups.add(group(type));
            }
        }
        return groups;
    }

    private static List<MapcssMapping.Rule> candidateRules(List<MapcssMapping.Rule> rules, Set<List<String>> includedGroups) {
        List<MapcssMapping.Rule> candidates = new ArrayList<>();
        for (MapcssMapping.Rule rule : rules) {
            if (isPoiClass(rule.type) || includedGroups.contains(group(rule.type))) {
                candidates.add(rule);
            }
        }
        return candidates;
    }

    private static List<PoiType> catalog(Set<List<String>> types, Map<List<String>, Set<String>> categoriesByType,
                                         TypeNames typeNames) {
        List<PoiType> catalog = new ArrayList<>();
        for (List<String> type : types) {
            Set<String> typeCategories = categoriesByType.get(type);
            if (typeCategories != null) {
                catalog.add(new PoiType(MapcssMapping.typeName(type), new ArrayList<>(typeCategories), names(type, typeNames)));
            }
        }
        catalog.sort(Comparator.comparing(PoiType::getType));
        return Collections.unmodifiableList(catalog);
    }

    private static Map<String, String> names(List<String> type, TypeNames typeNames) {
        Map<String, String> names = new HashMap<>();
        for (String language : NAME_LANGUAGES) {
            String name = typeNames.name(type, language);
            if (name != null) {
                names.put(language, name);
            }
        }
        return names;
    }

    public static OrganicMapsPoiClassifier load(Path dataDirectory) throws IOException {
        PoiRules poiRules;
        try (BufferedReader reader = resourceReader(RULES_RESOURCE)) {
            poiRules = PoiRules.parse(reader);
        }
        TagReplacer tagReplacer;
        try (BufferedReader reader = Files.newBufferedReader(dataDirectory.resolve(REPLACED_TAGS_FILE))) {
            tagReplacer = TagReplacer.parse(reader);
        }
        List<MapcssMapping.Rule> rules;
        try (BufferedReader reader = Files.newBufferedReader(dataDirectory.resolve(MAPPING_FILE))) {
            rules = MapcssMapping.parseRules(reader);
        }
        Categories categories;
        try (BufferedReader reader = Files.newBufferedReader(dataDirectory.resolve(CATEGORIES_FILE))) {
            categories = Categories.parse(reader);
        }
        TypeNames typeNames = TypeNames.EMPTY;
        Path typeNamesFile = dataDirectory.resolve(TYPE_NAMES_FILE);
        if (Files.exists(typeNamesFile)) {
            try (BufferedReader reader = Files.newBufferedReader(typeNamesFile)) {
                typeNames = TypeNames.parse(reader, NAME_LANGUAGES);
            }
        } else {
            log.warn("{} not found, POI type catalog will have no names", typeNamesFile);
        }
        OrganicMapsPoiClassifier classifier = new OrganicMapsPoiClassifier(tagReplacer, rules, categories, typeNames, poiRules);
        if (!classifier.unknownRuleTypes.isEmpty()) {
            log.warn("Types of {} are not in {}: {}", RULES_RESOURCE, MAPPING_FILE, classifier.unknownRuleTypes);
        }
        log.info("Loaded {} Organic Maps POI types from {}", classifier.poiTypes.size(), dataDirectory);
        return classifier;
    }

    public List<String> poiTypes(Collection<Tag> tags) {
        List<List<String>> matched = matchPoiTypes(tags);
        List<String> names = new ArrayList<>(matched.size());
        for (List<String> type : matched) {
            names.add(MapcssMapping.typeName(type));
        }
        return names;
    }

    public List<Tag> poiTags(Collection<Tag> tags) {
        List<List<String>> matched = matchPoiTypes(tags);
        if (matched.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<>(matched.size());
        Set<String> categories = new TreeSet<>();
        for (List<String> type : matched) {
            names.add(MapcssMapping.typeName(type));
            categories.addAll(poiCategories.get(type));
        }
        Tag poiTag = new Tag(POI_TAG, toArrayLiteral(names));
        if (categories.isEmpty()) {
            return Collections.singletonList(poiTag);
        }
        return List.of(poiTag, new Tag(POI_CATEGORY_TAG, toArrayLiteral(categories)));
    }

    public List<PoiType> getPoiTypes() {
        return poiTypes;
    }

    Set<String> getUnknownRuleTypes() {
        return unknownRuleTypes;
    }

    public static String toArrayLiteral(Collection<String> values) {
        StringBuilder literal = new StringBuilder("{");
        for (String value : values) {
            if (literal.length() > 1) {
                literal.append(',');
            }
            if (isPlainArrayElement(value)) {
                literal.append(value);
            } else {
                literal.append('"');
                for (int i = 0; i < value.length(); i++) {
                    char c = value.charAt(i);
                    if (c == '"' || c == '\\') {
                        literal.append('\\');
                    }
                    literal.append(c);
                }
                literal.append('"');
            }
        }
        return literal.append('}').toString();
    }

    private List<List<String>> matchPoiTypes(Collection<Tag> tags) {
        if (tags == null || tags.isEmpty()) {
            return Collections.emptyList();
        }
        List<MutableTag> objectTags = new ArrayList<>(tags.size());
        boolean mayMatch = mapping.hasUnindexedRules();
        for (Tag tag : tags) {
            String key = tag.getKey() != null ? tag.getKey().trim() : "";
            String value = tag.getValue() != null ? tag.getValue().trim() : "";
            if (!key.isEmpty() && !value.isEmpty()) {
                objectTags.add(new MutableTag(key, value));
                mayMatch = mayMatch || mapping.isIndexed(key, value) || tagReplacer.replaces(key, value);
            }
        }
        if (!mayMatch) {
            return Collections.emptyList();
        }
        tagReplacer.apply(objectTags);
        List<List<String>> matched = mapping.match(objectTags);
        List<List<String>> result = new ArrayList<>(matched.size());
        for (List<String> type : matched) {
            if (poiCategories.containsKey(type)) {
                result.add(type);
            }
        }
        return result;
    }

    private static Optional<Set<String>> poiCategories(List<String> type, Categories categories, PoiRules poiRules) {
        String name = MapcssMapping.typeName(type);
        if (poiRules.isExcluded(name)) {
            return Optional.empty();
        }
        Optional<Set<String>> typeCategories = categories.categoriesOf(type);
        Optional<Set<String>> includedCategories = poiRules.includedCategories(name);
        if (includedCategories.isPresent()) {
            Set<String> result = new TreeSet<>(includedCategories.get());
            typeCategories.ifPresent(result::addAll);
            return Optional.of(Collections.unmodifiableSet(result));
        }
        return isPoiClass(type) ? typeCategories : Optional.empty();
    }

    private static boolean isPoiClass(List<String> type) {
        return ONE_LEVEL_POI_CLASSES.contains(type.get(0))
                || (type.size() >= 2 && TWO_LEVEL_POI_TYPES.contains(type.get(0) + '-' + type.get(1)));
    }

    private static List<String> group(List<String> type) {
        return type.subList(0, Math.min(2, type.size()));
    }

    private static boolean isPlainArrayElement(String value) {
        if (value.isEmpty() || "NULL".equalsIgnoreCase(value)) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '{' || c == '}' || c == ',' || c == '"' || c == '\\' || Character.isWhitespace(c)) {
                return false;
            }
        }
        return true;
    }

    private static BufferedReader resourceReader(String resource) {
        InputStream stream = OrganicMapsPoiClassifier.class.getResourceAsStream(resource);
        if (stream == null) {
            throw new IllegalArgumentException("Resource not found: " + resource);
        }
        return new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }
}
