package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.output.ResourceCopier;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ExternalProcessing {
    private static final Logger log = LoggerFactory.getLogger(ExternalProcessing.class);
    public static final String MULTIPOLYGON_SOURCE_TSV = "/multipolygon/source.tsv";
    private static final int GETID_MISSING_OBJECTS_EXIT_CODE = 1;

    private final boolean invokeDockerCommand;

    public ExternalProcessing(boolean invokeDockerCommand) {
        this.invokeDockerCommand = invokeDockerCommand;
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
        List<String> command = osmiumCommand(sourcePbfFile, false);
        Collections.addAll(command, "add-locations-to-ways", sourcePbfFile.getName(), "-v",
                "--output-format", "pbf,pbf_compression=none",
                preserveAllNodes ? "--keep-untagged-nodes" : "--keep-member-nodes",
                "-i", getIndexType(sourcePbfFile), "-o", "-");
        return command;
    }

    public PipedProcess startMultipolygonExport(File sourcePbfFile) throws IOException {
        return PipedProcess.start(multipolygonExportCommand(sourcePbfFile), new File(sourcePbfFile.getParent()),
                ResourceCopier.resourceBytes("/" + ResourceCopier.OSMIUM_EXPORT_JSON));
    }

    List<String> multipolygonExportCommand(File sourcePbfFile) {
        List<String> command = osmiumCommand(sourcePbfFile, true);
        Collections.addAll(command, "export", "-e", "--config=/dev/stdin", "-i", getIndexType(sourcePbfFile),
                "--geometry-types", "polygon", "-v", "-f", "pg", "-x", "tags_type=hstore",
                sourcePbfFile.getName(), "-o", "-");
        return command;
    }

    public void extractRelations(File sourcePbfFile, Collection<Long> relationIds, File outputPbfFile)
            throws IOException, InterruptedException {
        StringBuilder ids = new StringBuilder();
        for (long relationId : relationIds) {
            ids.append('r').append(relationId).append('\n');
        }
        try (PipedProcess getid = PipedProcess.start(extractRelationsCommand(sourcePbfFile, outputPbfFile),
                new File(sourcePbfFile.getParent()), ids.toString().getBytes(StandardCharsets.US_ASCII))) {
            getid.waitForSuccessUpTo(GETID_MISSING_OBJECTS_EXIT_CODE);
        }
    }

    List<String> extractRelationsCommand(File sourcePbfFile, File outputPbfFile) {
        List<String> command = osmiumCommand(sourcePbfFile, true, outputPbfFile.getAbsoluteFile().getParent());
        Collections.addAll(command, "getid", "-r", "--overwrite", "-i", "-", "-v", "-f", "pbf",
                sourcePbfFile.getAbsolutePath(), "-o", outputPbfFile.getAbsolutePath());
        return command;
    }

    private List<String> osmiumCommand(File sourcePbfFile, boolean standardInput, String... extraMounts) {
        String basePath = sourcePbfFile.getAbsoluteFile().getParent();
        List<String> command = new ArrayList<>();
        if (invokeDockerCommand) {
            Collections.addAll(command, "docker", "run");
            if (standardInput) {
                command.add("-i");
            }
            Collections.addAll(command, "--rm", "--log-driver", "none", "-w", basePath, "-v", basePath + ":" + basePath);
            for (String mount : extraMounts) {
                if (!mount.equals(basePath)) {
                    Collections.addAll(command, "-v", mount + ":" + mount);
                }
            }
            command.add("mschilde/osmium-tool");
        }
        command.add("/usr/bin/osmium");
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
