package com.github.isuhorukov.osm.pgsnapshot.pipeline;

import com.beust.jcommander.JCommander;
import com.github.isuhorukov.osm.pgsnapshot.CliParameters;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class TagExtensionsTest {

    private static TagExtensions parse(String... args) {
        TagExtensions extensions = TagExtensions.discover();
        JCommander.Builder builder = JCommander.newBuilder().addObject(new CliParameters());
        extensions.addParameters(builder);
        builder.build().parse(args);
        return extensions;
    }

    @Test
    void discover_extensionsFromServiceFiles() {
        List<TagExtension> all = TagExtensions.discover().getAll();
        assertTrue(all.stream().anyMatch(TestTagExtension.class::isInstance), all.toString());
    }

    @Test
    void getEnabled_extensionWithParsedOption() {
        List<TagExtension> enabled = parse("-source_pbf", "a.osm.pbf", "-test_tag_value", "v").getEnabled();
        assertEquals(1, enabled.size());
        assertInstanceOf(TestTagExtension.class, enabled.get(0));
    }

    @Test
    void getEnabled_withoutExtensionOptions_empty() {
        assertTrue(parse("-source_pbf", "a.osm.pbf").getEnabled().isEmpty());
    }

    @Test
    void getAll_sortedByName() {
        TagExtensions extensions = new TagExtensions(List.of(
                new TestTagExtension("b", "is.b", "v"), new TestTagExtension("a", "is.a", null)));
        assertEquals(List.of("a", "b"), extensions.getAll().stream().map(TagExtension::getName).collect(Collectors.toList()));
        assertEquals(List.of("b"), extensions.getEnabled().stream().map(TagExtension::getName).collect(Collectors.toList()));
    }
}
