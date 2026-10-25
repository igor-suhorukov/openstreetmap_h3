package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import com.github.isuhorukov.osm.pgsnapshot.extension.PostgresqlResources;
import com.github.isuhorukov.osm.pgsnapshot.output.DirectoryPostgresqlResources;
import com.github.isuhorukov.osm.pgsnapshot.output.ResultLayout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class JdbcPostgresqlResources implements PostgresqlResources {

    private final InputFiles inputFiles;
    private final List<String> importScripts = new ArrayList<>();

    JdbcPostgresqlResources(InputFiles inputFiles) {
        this.inputFiles = inputFiles;
    }

    @Override
    public void addStaticFile(String name, byte[] content) {
        inputFiles.add(ResultLayout.STATIC_DIR + "/" + DirectoryPostgresqlResources.checkedFileName(name), content);
    }

    @Override
    public void addImportScript(String name, String script) {
        DirectoryPostgresqlResources.checkedFileName(name);
        importScripts.add(script);
    }

    List<String> getImportScripts() {
        return Collections.unmodifiableList(importScripts);
    }
}
