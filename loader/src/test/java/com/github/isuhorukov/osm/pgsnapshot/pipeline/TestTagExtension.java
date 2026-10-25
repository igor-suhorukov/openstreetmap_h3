package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.beust.jcommander.Parameter;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TestTagExtension implements TagExtension {

    private final String name;
    private final String key;

    @Parameter(names = {"-test_tag_value"}, description = "Value of is.test tag of the test extension")
    private String value;

    public TestTagExtension() {
        this("test", "is.test", null);
    }

    TestTagExtension(String name, String key, String value) {
        this.name = name;
        this.key = key;
        this.value = value;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getParameters() {
        return this;
    }

    @Override
    public boolean isEnabled() {
        return value != null;
    }

    @Override
    public List<Tag> additionalTags(Entity entity) {
        if ("fail".equals(value)) {
            throw new IllegalStateException("test extension failure");
        }
        if ("error".equals(value)) {
            throw new LinkageError("test extension error");
        }
        boolean amenity = entity.getTags().stream().anyMatch(tag -> "amenity".equals(tag.getKey()));
        return amenity ? List.of(new Tag(key, value + '-' + entity.getType())) : Collections.emptyList();
    }

    @Override
    public String getFingerprint() {
        return "value=" + value;
    }

    @Override
    public Set<String> multipolygonTagKeys() {
        return Set.of(key);
    }
}
