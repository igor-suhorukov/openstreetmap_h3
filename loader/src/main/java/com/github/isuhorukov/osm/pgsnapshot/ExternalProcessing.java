package com.github.isuhorukov.osm.pgsnapshot;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class ExternalProcessing {
    private static final Logger log = LoggerFactory.getLogger(ExternalProcessing.class);
    public static final String MULTIPOLYGON_SOURCE_TSV = "/multipolygon/source.tsv";
    private static final long DUCKDB_DEFAULT_MAX_LINE_SIZE = 2_000_000;
    private static final long DUCKDB_DEFAULT_BUFFER_SIZE = 32_000_000;

    private final boolean invokeDockerCommand;

    public ExternalProcessing(boolean invokeDockerCommand) {
        this.invokeDockerCommand = invokeDockerCommand;
    }

    public long exportMultipolygons(File sourcePbfFile, File resultDirectory, boolean saveArrow)
            throws IOException, InterruptedException {
        long osmiumExportStart = System.currentTimeMillis();
        executeMultipolygonExport(sourcePbfFile, resultDirectory.getName(), resultDirectory.getParent(),
                getIndexType(sourcePbfFile));
        if (saveArrow) {
            transformMultipolygonToParquet(resultDirectory);
        }
        return System.currentTimeMillis() - osmiumExportStart;
    }

    public long splitMultipolygonsAndWriteScripts(File resultDirectory, int scriptCount, long multipolygonCount)
            throws IOException, InterruptedException {
        long partSize = multipolygonCount / scriptCount + 1;
        String resultDirFullPath = resultDirectory.getPath();
        String multipolygonSourceTsv = resultDirFullPath + MULTIPOLYGON_SOURCE_TSV;
        String splitCommand = "cat " + multipolygonSourceTsv + " | grep $'\\trelation\\t' | split -l " + partSize + " - " + resultDirFullPath + "/multipolygon/multipolygon_";
        log.info(splitCommand);
        long multipolygonSplitStart = System.currentTimeMillis();
        runCliCommand(new String[]{"/bin/bash", "-c", splitCommand});
        long splitTime = System.currentTimeMillis() - multipolygonSplitStart;

        try {
            Files.delete(new File(multipolygonSourceTsv).toPath());
        } catch (IOException e) {
            log.warn("Failed to delete temporary file: {}", multipolygonSourceTsv, e);
        }

        generateMultipolygonCopyScripts(resultDirFullPath);
        return splitTime;
    }

    // Output path is derived from server-controlled resultDirectory, not from external user input.
    @SuppressWarnings("java:S2077")
    public static void transformMultipolygonToParquet(File resultDirectory) {
        String multipolygonSourceTsv = resultDirectory.getAbsolutePath() + MULTIPOLYGON_SOURCE_TSV;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("COPY (SELECT  column2 id, from_hex(column0) wkb," +
                    "'{'||replace(replace(column3,'\"=>\"','\":\"'),'\\\\','\\')||'}' tags_json FROM read_csv('"
                    + multipolygonSourceTsv +
                    "', HEADER=false, " +
                    "columns=STRUCT_PACK(column0 := 'VARCHAR', column1 := 'VARCHAR', column2 := 'BIGINT', column3 := 'VARCHAR')," +
                    "delim=E'\\t',escape=E'\\\\',quote='',AUTO_DETECT='false'," +
                    csvLineSizeOptions(new File(multipolygonSourceTsv)) + ") where column1='relation') TO '"
                    + resultDirectory.getAbsolutePath() + "/arrow/multipolygon.parquet' (FORMAT 'PARQUET', CODEC 'ZSTD')");
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    static String csvLineSizeOptions(File tsvFile) throws IOException {
        long maxLineSize = Math.max(DUCKDB_DEFAULT_MAX_LINE_SIZE, longestLineLength(tsvFile) + 1);
        return "max_line_size=" + maxLineSize + ",buffer_size=" + Math.max(DUCKDB_DEFAULT_BUFFER_SIZE, 2 * maxLineSize);
    }

    static long longestLineLength(File file) throws IOException {
        long longest = 0;
        long current = 0;
        byte[] buffer = new byte[1 << 20];
        try (InputStream inputStream = new FileInputStream(file)) {
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                for (int i = 0; i < read; i++) {
                    if (buffer[i] == '\n') {
                        longest = Math.max(longest, current);
                        current = 0;
                    } else {
                        current++;
                    }
                }
            }
        }
        return Math.max(longest, current);
    }

    public void executeMultipolygonExport(File sourcePbfFile, String resultDirName, String basePath,
                                          String indexType) throws IOException, InterruptedException {
        String multipolygonCommand = (invokeDockerCommand ?
                ("docker run -w " + basePath + " -v " + basePath + ":" + basePath +
                        " mschilde/osmium-tool ") : "") + "/usr/bin/osmium export  -e --config=" + resultDirName +
                "/static/osmium_export.json --fsync -i " + indexType +
                " --geometry-types polygon  -v -f pg -x tags_type=hstore " + sourcePbfFile.getName() +
                " -o " + (resultDirName + MULTIPOLYGON_SOURCE_TSV);
        runCliCommand(multipolygonCommand, basePath);
    }

    static void generateMultipolygonCopyScripts(String resultDirFullPath) throws IOException {
        List<String> multipolyParts = Arrays.stream(
                Objects.requireNonNull(new File(resultDirFullPath, "multipolygon").listFiles())).
                map(File::getName).collect(Collectors.toList());
        for (String multipolyPart : multipolyParts) {
            String copyCommand = "\\timing on\ncopy preimport_multipolygon FROM '/input/multipolygon/" + multipolyPart + "';";
            try (FileOutputStream copyScriptOutput = new FileOutputStream(String.format("%s/sql/y_multipoly_%s.sql",
                    resultDirFullPath, multipolyPart.replace("multipolygon_", "")))) {
                IOUtils.write(copyCommand, copyScriptOutput, StandardCharsets.UTF_8);
            }
        }
    }

    public PipedProcess startAddLocationsToWays(File sourcePbfFile, boolean preserveAllNodes) throws IOException {
        return PipedProcess.start(addLocationsToWaysCommand(sourcePbfFile, preserveAllNodes),
                new File(sourcePbfFile.getParent()));
    }

    List<String> addLocationsToWaysCommand(File sourcePbfFile, boolean preserveAllNodes) {
        String basePath = sourcePbfFile.getParent();
        List<String> command = new ArrayList<>();
        if (invokeDockerCommand) {
            Collections.addAll(command, "docker", "run", "--rm", "--log-driver", "none",
                    "-w", basePath, "-v", basePath + ":" + basePath, "mschilde/osmium-tool");
        }
        Collections.addAll(command, "/usr/bin/osmium", "add-locations-to-ways", sourcePbfFile.getName(), "-v",
                "--output-format", "pbf,pbf_compression=none",
                preserveAllNodes ? "--keep-untagged-nodes" : "--keep-member-nodes",
                "-i", getIndexType(sourcePbfFile), "-o", "-");
        return command;
    }

    private static void runCliCommand(String[] cmdarray) throws IOException, InterruptedException {
        runProcess(Runtime.getRuntime().exec(cmdarray), Arrays.toString(cmdarray));
    }

    public static String getIndexType(File sourcePbfFile) {
        long sourceFileLength = sourcePbfFile.length();
        long maxMemory = Runtime.getRuntime().maxMemory();
        return sourceFileLength > maxMemory ? "dense_file_array" : "sparse_mem_array";
    }

    private static void runCliCommand(String command, String basePath) throws IOException, InterruptedException {
        runProcess(Runtime.getRuntime().exec(commandArray(command), new String[0], new File(basePath)), command);
    }

    private static String[] commandArray(String command) {
        StringTokenizer tokenizer = new StringTokenizer(command);
        String[] commandArray = new String[tokenizer.countTokens()];
        for (int i = 0; tokenizer.hasMoreTokens(); i++) {
            commandArray[i] = tokenizer.nextToken();
        }
        return commandArray;
    }

    static void runProcess(Process process, String commandDescription) throws IOException, InterruptedException {
        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            process.destroy();
            throw e;
        }
        if (log.isInfoEnabled()) {
            log.info(IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8));
        }
        if(log.isErrorEnabled()) {
            log.error(IOUtils.toString(process.getErrorStream(), StandardCharsets.UTF_8));
        }
        if (exitCode != 0) {
            throw new IllegalStateException("exit code " + exitCode + " command: " + commandDescription);
        }
    }
}
