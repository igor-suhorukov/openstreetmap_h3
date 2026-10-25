package com.github.isuhorukov.osm.pgsnapshot.extension;

import java.io.IOException;

public interface PostgresqlResources {

    void addStaticFile(String name, byte[] content) throws IOException;

    void addImportScript(String name, String script) throws IOException;
}
