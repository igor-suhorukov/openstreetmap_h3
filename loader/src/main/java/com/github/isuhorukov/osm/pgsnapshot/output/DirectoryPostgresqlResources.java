package com.github.isuhorukov.osm.pgsnapshot.output;

import com.github.isuhorukov.osm.pgsnapshot.extension.PostgresqlResources;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class DirectoryPostgresqlResources implements PostgresqlResources {

    private final File staticDirectory;
    private final File sqlDirectory;

    public DirectoryPostgresqlResources(File resultDirectory) {
        this.staticDirectory = new File(resultDirectory, ResultLayout.STATIC_DIR);
        this.sqlDirectory = new File(resultDirectory, ResultLayout.SQL_DIR);
    }

    @Override
    public void addStaticFile(String name, byte[] content) throws IOException {
        Files.write(new File(staticDirectory, checkedFileName(name)).toPath(), content);
    }

    @Override
    public void addImportScript(String name, String script) throws IOException {
        Files.write(new File(sqlDirectory, checkedFileName(name)).toPath(), script.getBytes(StandardCharsets.UTF_8));
    }

    public static String checkedFileName(String name) {
        if (name == null || name.isEmpty() || !new File(name).getName().equals(name) || ".".equals(name) || "..".equals(name)) {
            throw new IllegalArgumentException("Resource name should be a plain file name: " + name);
        }
        return name;
    }
}
