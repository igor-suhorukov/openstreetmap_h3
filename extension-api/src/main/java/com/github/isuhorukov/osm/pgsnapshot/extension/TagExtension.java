package com.github.isuhorukov.osm.pgsnapshot.extension;

import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public interface TagExtension {

    String getName();

    default Object getParameters() {
        return null;
    }

    boolean isEnabled();

    default void init() throws IOException {
    }

    List<Tag> additionalTags(Entity entity);

    default Set<String> multipolygonTagKeys() {
        return Collections.emptySet();
    }

    default void writePostgresqlResources(File staticDirectory, File sqlDirectory) throws IOException {
    }
}
