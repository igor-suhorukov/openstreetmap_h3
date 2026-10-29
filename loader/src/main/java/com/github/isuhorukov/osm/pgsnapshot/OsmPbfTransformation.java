package com.github.isuhorukov.osm.pgsnapshot;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.ParameterException;
import com.github.luben.zstd.Zstd;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import com.github.isuhorukov.osm.pgsnapshot.jdbc.UpdateRefusedException;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.Pipeline;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.TagExtensions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class OsmPbfTransformation {

    private static final Logger log = LoggerFactory.getLogger(OsmPbfTransformation.class);

    private static final String CONSOLE_LOGGER = "com.github.isuhorukov.osm.pgsnapshot.console";
    private static final Logger console = LoggerFactory.getLogger(CONSOLE_LOGGER);

    public static final int INVALID_ARGUMENTS_EXIT_CODE = 2;
    public static final int UPDATE_REFUSED_EXIT_CODE = 3;

    public static void main(String[] args) throws IOException, InterruptedException {
        int exitCode = run(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args) throws IOException, InterruptedException {
        TagExtensions tagExtensions = TagExtensions.discover();
        CliParameters parameters = new CliParameters();
        JCommander.Builder builder = JCommander.newBuilder().addObject(parameters);
        tagExtensions.addParameters(builder);
        JCommander jc = builder.build();
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            log.warn(e.getMessage());
            jc.usage();
            List<String> unknownOptions = unknownOptions(jc, args);
            console.error(unknownOptions.isEmpty() ? e.getMessage() : extensionHint(unknownOptions, tagExtensions));
            return INVALID_ARGUMENTS_EXIT_CODE;
        }
        if (parameters.isHelp()) {
            jc.usage();
            return 0;
        }
        if (!parameters.isSaveArrow() && !parameters.isSavePostgresqlTsv()) {
            throw new IllegalArgumentException("result_in_tsv or/and arrow_format parameters should be specified");
        }
        if (parameters.isLoadPostgresqlJdbc() && (parameters.isSaveArrow() || parameters.isCollectOnlyStat())) {
            throw new IllegalArgumentException("pg_jdbc_url can't be combined with arrow_format or collect_only_statistics");
        }
        String optionsError = optionsError(parameters);
        if (optionsError != null) {
            jc.usage();
            console.error(optionsError);
            return INVALID_ARGUMENTS_EXIT_CODE;
        }
        try {
            new Pipeline(parameters, tagExtensions.getEnabled()).run();
        } catch (UpdateRefusedException e) {
            console.error(e.getMessage());
            return UPDATE_REFUSED_EXIT_CODE;
        }
        return 0;
    }

    static String optionsError(CliParameters parameters) {
        String updateOptionsError = updateOptionsError(parameters);
        if (updateOptionsError != null) {
            return updateOptionsError;
        }
        if (parameters.isGeoParquetVersionSet() && parameters.getArrowFormat() != ArrowFormat.PARQUET) {
            return "-geoparquet_version requires -arrow_format PARQUET";
        }
        if (parameters.isParquetCompressionSet() && !parameters.isSaveArrow()) {
            return "-parquet_compression requires -arrow_format";
        }
        return spillOptionsError(parameters);
    }

    static String spillOptionsError(CliParameters parameters) {
        if ((parameters.isSpillCompressionSet() || parameters.isSpillZstdLevelSet()) && !parameters.isSaveArrow()) {
            return "-spill_compression and -spill_zstd_level require -arrow_format";
        }
        if (!parameters.isSpillZstdLevelSet()) {
            return null;
        }
        if (parameters.getSpillCompression() != SpillCompression.ZSTD) {
            return "-spill_zstd_level requires -spill_compression ZSTD";
        }
        int level = parameters.getSpillZstdLevel();
        if (level < Zstd.minCompressionLevel() || level > Zstd.maxCompressionLevel()) {
            return "-spill_zstd_level must be between " + Zstd.minCompressionLevel() + " and "
                    + Zstd.maxCompressionLevel();
        }
        return null;
    }

    static String updateOptionsError(CliParameters parameters) {
        if (parameters.isUpdatable() && parameters.isColumnarStorage()) {
            return "-updatable and -update can't be combined with -columnar_storage: columnar tables don't support UPDATE and DELETE";
        }
        if (parameters.isUpdate() && !parameters.isLoadPostgresqlJdbc()) {
            return "-update requires -pg_jdbc_url";
        }
        if (parameters.isUpdate() && (parameters.isSaveArrow() || parameters.isCollectOnlyStat())) {
            return "-update can't be combined with -arrow_format or -collect_only_statistics";
        }
        return null;
    }

    private static List<String> unknownOptions(JCommander jc, String[] args) {
        Set<String> knownOptions = new HashSet<>();
        for (ParameterDescription description : jc.getParameters()) {
            knownOptions.addAll(Arrays.asList(description.getParameter().names()));
        }
        List<String> unknownOptions = new ArrayList<>();
        for (String arg : args) {
            if (arg.matches("--?[A-Za-z].*") && !knownOptions.contains(arg)) {
                unknownOptions.add(arg);
            }
        }
        return unknownOptions;
    }

    private static String extensionHint(List<String> unknownOptions, TagExtensions tagExtensions) {
        String loadedExtensions = tagExtensions.getAll().isEmpty() ? "none"
                : tagExtensions.getAll().stream().map(TagExtension::getName).collect(Collectors.joining(", "));
        return "Unknown option: " + String.join(", ", unknownOptions) + "\n"
                + "Options of extensions are available when the extension jar is on loader.path:\n"
                + "  java -Dloader.path=<extension jar or directory> -jar <loader jar> ...\n"
                + "  or the LOADER_PATH environment variable (/plugins in the Docker image)\n"
                + "Loaded extensions: " + loadedExtensions;
    }
}
