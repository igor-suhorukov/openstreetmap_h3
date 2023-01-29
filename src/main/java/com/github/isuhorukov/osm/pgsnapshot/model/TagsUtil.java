package com.github.isuhorukov.osm.pgsnapshot.model;

import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TagsUtil {
    public static Map<String, String> tagsToMap(Collection<Tag> entityTags) {
        if (entityTags != null && !entityTags.isEmpty()){
            return entityTags.stream()
                    .filter(tag -> tag.getKey().trim().length() > 0 && tag.getValue().trim().length() > 0)
                    .collect(Collectors.toMap(Tag::getKey, Tag::getValue, (s, s2) -> s, TreeMap::new));
        } else {
            return null;
        }
    }
}
