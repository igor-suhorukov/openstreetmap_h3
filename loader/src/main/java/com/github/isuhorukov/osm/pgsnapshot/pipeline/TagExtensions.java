package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.beust.jcommander.JCommander;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;

import java.util.*;
import java.util.stream.Collectors;

public class TagExtensions {

    private final List<TagExtension> extensions;

    TagExtensions(List<TagExtension> extensions) {
        List<TagExtension> sorted = new ArrayList<>(extensions);
        sorted.sort(Comparator.comparing(TagExtension::getName));
        this.extensions = Collections.unmodifiableList(sorted);
    }

    public static TagExtensions discover() {
        List<TagExtension> extensions = new ArrayList<>();
        for (TagExtension extension : ServiceLoader.load(TagExtension.class)) {
            extensions.add(extension);
        }
        return new TagExtensions(extensions);
    }

    public void addParameters(JCommander.Builder builder) {
        for (TagExtension extension : extensions) {
            Object parameters = extension.getParameters();
            if (parameters != null) {
                builder.addObject(parameters);
            }
        }
    }

    public List<TagExtension> getAll() {
        return extensions;
    }

    public List<TagExtension> getEnabled() {
        return extensions.stream().filter(TagExtension::isEnabled).collect(Collectors.toList());
    }
}
