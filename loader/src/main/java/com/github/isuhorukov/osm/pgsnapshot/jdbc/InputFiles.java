package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.output.ResourceCopier;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class InputFiles {

    static final String INPUT_DIRECTORY = "/input/";

    @FunctionalInterface
    interface Source {
        InputStream open() throws IOException;
    }

    private final Map<String, Source> files = new ConcurrentHashMap<>();

    void add(String path, byte[] content) {
        files.put(path, () -> new ByteArrayInputStream(content));
    }

    void add(String path, String content) {
        add(path, content.getBytes(StandardCharsets.UTF_8));
    }

    void addResource(String path, String classpath) {
        files.put(path, () -> ResourceCopier.openResource(classpath));
    }

    InputStream open(String inputPath) throws IOException {
        Source source = files.get(relativePath(inputPath));
        if (source == null) {
            throw new IllegalArgumentException("No content for " + inputPath);
        }
        return source.open();
    }

    String read(String inputPath) throws IOException {
        try (InputStream inputStream = open(inputPath)) {
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    static String relativePath(String inputPath) {
        if (!inputPath.startsWith(INPUT_DIRECTORY)) {
            throw new IllegalArgumentException("Path should start with " + INPUT_DIRECTORY + ": " + inputPath);
        }
        return inputPath.substring(INPUT_DIRECTORY.length());
    }
}
