package com.github.isuhorukov.osm.pgsnapshot;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OsmPbfTransformationTest {

    public static final String TEST_DATA_URL = "https://download.geofabrik.de/asia/maldives-240825.osm.pbf";

    @Test
    void dockerSmokeTest() throws Exception {
        try (GenericContainer<?> container = new GenericContainer<>(
                new ImageFromDockerfile("openstreetmap_h3:1.0", false)
                        .withFileFromPath("Dockerfile", Paths.get("Dockerfile"))
                        .withFileFromPath("pom.xml",Paths.get("pom.xml"))
                        .withFileFromPath("src/main",Paths.get("src/main")))){

            String prefix = "maldives";
            File pbfFile = getFileForTest(prefix, TEST_DATA_URL);

            container.withWorkingDirectory(pbfFile.getParent());

            String containerPath = "/input";
            container.addFileSystemBind(pbfFile.getParent(), containerPath, BindMode.READ_WRITE, SelinuxContext.SHARED);

            container.setCommand("-source_pbf", containerPath+"/"+pbfFile.getName());
            container.start();
            container.execInContainer("chmod", "-R", "777", containerPath+"/"+prefix+"*");

            WaitingConsumer waitingConsumer = new WaitingConsumer();

            Consumer<OutputFrame> composedConsumer = new Slf4jLogConsumer(LoggerFactory.getLogger(this.getClass())).andThen(waitingConsumer);
            container.followOutput(composedConsumer);

            waitingConsumer.waitUntil(frame ->
                    frame.getUtf8String().contains("grep $'\\trelation\\t'"), 2, TimeUnit.MINUTES);

            File[] files = new File(pbfFile.getParent(),
                    pbfFile.getName().replace(".osm.pbf", "_loc_ways")).listFiles();

            Set<String> dir = Set.of("import_related_metadata", "multipolygon", "nodes", "relations", "sql", "static", "ways");
            assertTrue(Arrays.stream(Objects.requireNonNull(files)).allMatch(file -> dir.contains(file.getName())));
            assertEquals("32767.tsv 339441\n" +
                    "24942.tsv 330117\n" +
                    "24940.tsv 1661967\n" +
                    "24929.tsv 879626\n" +
                    "24926.tsv 4454768\n" +
                    "24924.tsv 3980897\n" +
                    "24922.tsv 60252\n" +
                    "24920.tsv 3291\n" +
                    "24913.tsv 1125913\n" +
                    "24901.tsv 2931113\n" +
                    "24900.tsv 577\n" +
                    "24899.tsv 4871323\n" +
                    "24898.tsv 11831359\n" +
                    "24897.tsv 2504406\n" +
                    "24896.tsv 2275154\n" +
                    "24621.tsv 2206646\n" +
                    "-31636.tsv 1847883\n" +
                    "-31635.tsv 3166183",
                    Arrays.stream(Objects.requireNonNull(
                            Arrays.stream(files).filter(file -> "ways".contains(file.getName())).
                                    findFirst().orElse(pbfFile).listFiles())).
                            map(file -> file.getName()+" "+file.length()).
                            sorted(Comparator.reverseOrder()).
                            collect(Collectors.joining("\n")));
            assertEquals("24942.tsv 38626\n" +
                    "24940.tsv 176971\n" +
                    "24938.tsv 121\n" +
                    "24929.tsv 117630\n" +
                    "24926.tsv 528302\n" +
                    "24924.tsv 206794\n" +
                    "24922.tsv 12096\n" +
                    "24920.tsv 363\n" +
                    "24918.tsv 456\n" +
                    "24914.tsv 456\n" +
                    "24913.tsv 79952\n" +
                    "24901.tsv 308874\n" +
                    "24899.tsv 235225\n" +
                    "24898.tsv 546327\n" +
                    "24897.tsv 111589\n" +
                    "24896.tsv 134003\n" +
                    "24845.tsv 320\n" +
                    "24841.tsv 684\n" +
                    "24692.tsv 229\n" +
                    "24689.tsv 458\n" +
                    "24621.tsv 157854\n" +
                    "24620.tsv 229\n" +
                    "24617.tsv 458\n" +
                    "24616.tsv 229\n" +
                    "24609.tsv 685\n" +
                    "24602.tsv 100\n" +
                    "-31640.tsv 369\n" +
                    "-31636.tsv 30044\n" +
                    "-31635.tsv 49716",
                    Arrays.stream(Objects.requireNonNull(
                                    Arrays.stream(files).filter(file -> "nodes".contains(file.getName())).
                                            findFirst().orElse(pbfFile).listFiles())).
                            map(file -> file.getName()+" "+file.length()).
                            sorted(Comparator.reverseOrder()).
                            collect(Collectors.joining("\n")));

            assertEquals("00001.tsv 230042\n" +
                            "00000.tsv 110092",
                    Arrays.stream(Objects.requireNonNull(
                                    Arrays.stream(files).filter(file -> "relations".contains(file.getName())).
                                            findFirst().orElse(pbfFile).listFiles())).
                            map(file -> file.getName()+" "+file.length()).
                            sorted(Comparator.reverseOrder()).
                            collect(Collectors.joining("\n")));

            assertTrue(pbfFile.delete());
        }
    }

    @NotNull
    private static File getFileForTest(String prefix, String url) throws IOException {
        File pbfFile = Files.createTempFile(prefix,"240825.osm.pbf").toFile();
        try (InputStream in = URI.create(url).toURL().openStream();
             FileOutputStream out = new FileOutputStream(pbfFile)) {
            IOUtils.copy(in, out);
        }
        return pbfFile;
    }

    @Test
    void localSmokeTest() throws Exception {
        File pbfFile = getFileForTest("maldives", TEST_DATA_URL);

        OsmPbfTransformation.main(new String[]{"-source_pbf", pbfFile.getAbsolutePath(), "-osmium_docker"});

        File[] files = new File(pbfFile.getParent(),
                pbfFile.getName().replace(".osm.pbf", "_loc_ways")).listFiles();

        Set<String> dir = Set.of("import_related_metadata", "multipolygon", "nodes", "relations", "sql", "static", "ways");
        assertTrue(Arrays.stream(Objects.requireNonNull(files)).allMatch(file -> dir.contains(file.getName())));
        assertEquals("32767.tsv 339441\n" +
                        "24942.tsv 330117\n" +
                        "24940.tsv 1661967\n" +
                        "24929.tsv 879626\n" +
                        "24926.tsv 4454768\n" +
                        "24924.tsv 3980897\n" +
                        "24922.tsv 60252\n" +
                        "24920.tsv 3291\n" +
                        "24913.tsv 1125913\n" +
                        "24901.tsv 2931113\n" +
                        "24900.tsv 577\n" +
                        "24899.tsv 4871323\n" +
                        "24898.tsv 11831359\n" +
                        "24897.tsv 2504406\n" +
                        "24896.tsv 2275154\n" +
                        "24621.tsv 2206646\n" +
                        "-31636.tsv 1847883\n" +
                        "-31635.tsv 3166183",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "ways".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName()+" "+file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("24942.tsv 38626\n" +
                        "24940.tsv 176971\n" +
                        "24938.tsv 121\n" +
                        "24929.tsv 117630\n" +
                        "24926.tsv 528302\n" +
                        "24924.tsv 206794\n" +
                        "24922.tsv 12096\n" +
                        "24920.tsv 363\n" +
                        "24918.tsv 456\n" +
                        "24914.tsv 456\n" +
                        "24913.tsv 79952\n" +
                        "24901.tsv 308874\n" +
                        "24899.tsv 235225\n" +
                        "24898.tsv 546327\n" +
                        "24897.tsv 111589\n" +
                        "24896.tsv 134003\n" +
                        "24845.tsv 320\n" +
                        "24841.tsv 684\n" +
                        "24692.tsv 229\n" +
                        "24689.tsv 458\n" +
                        "24621.tsv 157854\n" +
                        "24620.tsv 229\n" +
                        "24617.tsv 458\n" +
                        "24616.tsv 229\n" +
                        "24609.tsv 685\n" +
                        "24602.tsv 100\n" +
                        "-31640.tsv 369\n" +
                        "-31636.tsv 30044\n" +
                        "-31635.tsv 49716",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "nodes".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName()+" "+file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));

        assertEquals("00001.tsv 230042\n" +
                        "00000.tsv 110092",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "relations".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName()+" "+file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));

        assertTrue(pbfFile.delete());
    }
}
