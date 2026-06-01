package com.github.isuhorukov.osm.pgsnapshot.model;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TagsUtil {
    private TagsUtil() {}

    public static Map<String, String> tagsToMap(Collection<Tag> entityTags) {
        if (entityTags != null && !entityTags.isEmpty()){
            return entityTags.stream()
                    .filter(tag -> !tag.getKey().trim().isEmpty() && !tag.getValue().trim().isEmpty())
                    .collect(Collectors.toMap(Tag::getKey, Tag::getValue, (s, s2) -> s, TreeMap::new));
        } else {
            return Collections.emptyMap();
        }
    }
}
