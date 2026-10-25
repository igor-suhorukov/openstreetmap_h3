package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ResultDirectoryImport {

    private ResultDirectoryImport() {
    }

    public static void importResultDirectory(Connection connection, File resultDirectory) throws IOException, SQLException {
        Path root = resultDirectory.toPath();
        InputFiles inputFiles = new InputFiles();
        for (Path file : files(root, Integer.MAX_VALUE)) {
            inputFiles.add(root.relativize(file).toString(), Files.readAllBytes(file));
        }
        PsqlRunner runner = new PsqlRunner(inputFiles);
        runner.run(connection, read(root.resolve("static/database_init.sql")));
        for (String directory : List.of("sql", "sql_finalize")) {
            for (Path script : files(root.resolve(directory), 1)) {
                runner.run(connection, read(script));
            }
        }
        runner.run(connection, read(root.resolve("static/database_after_init.sql")));
    }

    private static List<Path> files(Path directory, int depth) throws IOException {
        try (Stream<Path> paths = Files.walk(directory, depth)) {
            return paths.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
    }

    private static String read(Path script) throws IOException {
        return new String(Files.readAllBytes(script), StandardCharsets.UTF_8);
    }
}
