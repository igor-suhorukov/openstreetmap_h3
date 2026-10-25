package com.github.isuhorukov.osm.pgsnapshot.poi;

import com.github.isuhorukov.osm.pgsnapshot.extension.PostgresqlResources;

import java.util.LinkedHashMap;
import java.util.Map;

class RecordingPostgresqlResources implements PostgresqlResources {

    final Map<String, byte[]> staticFiles = new LinkedHashMap<>();
    final Map<String, String> importScripts = new LinkedHashMap<>();

    @Override
    public void addStaticFile(String name, byte[] content) {
        staticFiles.put(name, content);
    }

    @Override
    public void addImportScript(String name, String script) {
        importScripts.put(name, script);
    }
}
