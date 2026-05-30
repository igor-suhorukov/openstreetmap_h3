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
import java.security.MessageDigest;
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

    public static final String TEST_DATA_URL = "https://download.geofabrik.de/asia/maldives-250101.osm.pbf";

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

            assertOpenstreetmapH3Result(pbfFile);
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

    void localSmokeTest() throws Exception {
        File pbfFile = getFileForTest("maldives", TEST_DATA_URL);

        OsmPbfTransformation.main(new String[]{"-source_pbf", pbfFile.getAbsolutePath()});

        assertOpenstreetmapH3Result(pbfFile);
    }

    private static void assertOpenstreetmapH3Result(File pbfFile) {
        File[] files = new File(pbfFile.getParent(),
                pbfFile.getName().replace(".osm.pbf", "_loc_ways")).listFiles();

        Set<String> dir = Set.of("import_related_metadata", "multipolygon", "nodes", "relations", "sql", "static", "ways");
        assertTrue(Arrays.stream(Objects.requireNonNull(files)).allMatch(file -> dir.contains(file.getName())));
        assertEquals("32767.tsv 352459\n" +
                        "24942.tsv 330117\n" +
                        "24940.tsv 1678431\n" +
                        "24929.tsv 1148459\n" +
                        "24926.tsv 4588112\n" +
                        "24924.tsv 4686054\n" +
                        "24922.tsv 62979\n" +
                        "24920.tsv 3291\n" +
                        "24913.tsv 1144576\n" +
                        "24901.tsv 2946252\n" +
                        "24900.tsv 577\n" +
                        "24899.tsv 4903916\n" +
                        "24898.tsv 12543046\n" +
                        "24897.tsv 2637343\n" +
                        "24896.tsv 2286433\n" +
                        "24621.tsv 2492893\n" +
                        "-31636.tsv 1851045\n" +
                        "-31635.tsv 3173062",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "ways".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName()+" "+file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("24942.tsv 38626\n" +
                        "24940.tsv 176971\n" +
                        "24938.tsv 121\n" +
                        "24929.tsv 94823\n" +
                        "24926.tsv 540313\n" +
                        "24924.tsv 214336\n" +
                        "24922.tsv 12237\n" +
                        "24920.tsv 363\n" +
                        "24918.tsv 456\n" +
                        "24914.tsv 456\n" +
                        "24913.tsv 79952\n" +
                        "24901.tsv 308873\n" +
                        "24899.tsv 237831\n" +
                        "24898.tsv 671932\n" +
                        "24897.tsv 112577\n" +
                        "24896.tsv 120888\n" +
                        "24845.tsv 320\n" +
                        "24841.tsv 684\n" +
                        "24692.tsv 229\n" +
                        "24689.tsv 458\n" +
                        "24621.tsv 157774\n" +
                        "24620.tsv 229\n" +
                        "24617.tsv 458\n" +
                        "24616.tsv 229\n" +
                        "24609.tsv 685\n" +
                        "24602.tsv 100\n" +
                        "-31640.tsv 369\n" +
                        "-31636.tsv 29278\n" +
                        "-31635.tsv 42839",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "nodes".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName()+" "+file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));

        assertEquals("00001.tsv 231770\n" +
                        "00000.tsv 112888",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "relations".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName()+" "+file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("multipolygon_ae 202656\n" +
                        "multipolygon_ad 1069162\n" +
                        "multipolygon_ac 511321\n" +
                        "multipolygon_ab 776112\n" +
                        "multipolygon_aa 740614",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "multipolygon".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName()+" "+file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));

        assertEquals("32767.tsv 596fd854008e73851dc09b843df4fa0b\n" +
                        "24942.tsv ba0ea13a314de09a3617aaa408bfaeeb\n" +
                        "24940.tsv 6394b7e4840cb59c86e94b79df493eb2\n" +
                        "24929.tsv 8524129cb4993f9a56ce74a36c65c6f8\n" +
                        "24926.tsv a851d9816e5b584e4bef7d9e1fa45849\n" +
                        "24924.tsv a16b41955b3dfd002517858e60f9939e\n" +
                        "24922.tsv 585247becea4fbb0f2846f700df68bdb\n" +
                        "24920.tsv cf9df7b5127f8bf5c8e55c06524d6125\n" +
                        "24913.tsv 6a8a52d5e40139514f41a89cc1283256\n" +
                        "24901.tsv a3e60949167ec099f094cb573e074a37\n" +
                        "24900.tsv 7850051b2f60c4797fa321e02d208b6c\n" +
                        "24899.tsv 4c5f27a96306c787631aeb69eb689073\n" +
                        "24898.tsv 6ab75ad956a58b56540b216e104504cf\n" +
                        "24897.tsv 7f8e87711ed8bae6960d1c10d056356b\n" +
                        "24896.tsv 89ff25c679846f687b608b7a1223a7e5\n" +
                        "24621.tsv 1ed6c4b99613b0c31e91aeaac11347d0\n" +
                        "-31636.tsv 56a0c4d5bbf4eb4a43bf4b3ee392689a\n" +
                        "-31635.tsv 6935cd5ef97d20e7b049da7f54b234b9",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "ways".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName() + " " + fileMd5(file)).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("24942.tsv 5a6de896f8cdeee5b5413f1479c1a3a6\n" +
                        "24940.tsv 710214f199bfb0814956aea2f0239a38\n" +
                        "24938.tsv 7d8da90d2027f2bbdc57f1600d0db0ec\n" +
                        "24929.tsv 9532ad6196e3357d5e97ee66abf7e1f6\n" +
                        "24926.tsv ad91c9f9ea7ac3feb851cb1f8b4b6c79\n" +
                        "24924.tsv 3692a1da2d3106adf2227b6f761fda09\n" +
                        "24922.tsv 6c1ce61ffd17a8a1bc3d5fe930208713\n" +
                        "24920.tsv fac5abe4199c08719a9f3d6f441b277a\n" +
                        "24918.tsv 71b1a6adc4390f8f9305b615f86231d2\n" +
                        "24914.tsv 26948acaab6c9dda02f6576443d8f598\n" +
                        "24913.tsv eaa0d5e0d750be1f97a7e04132d3ee5d\n" +
                        "24901.tsv 04149b3e3189e12f86264850384bc35c\n" +
                        "24899.tsv 47f409cb55ccfeaf342cddbdd047c4bf\n" +
                        "24898.tsv c3994fa112cc340d102500b09a710a04\n" +
                        "24897.tsv 13211cccad97651424f33fd43cc0ca79\n" +
                        "24896.tsv 313de9d43aaf2791079a36ae72f2c5c7\n" +
                        "24845.tsv 57045f9e2e95408238255b22f63a0981\n" +
                        "24841.tsv ccbc3f5445cee050b37ee8ccc6967fa0\n" +
                        "24692.tsv c38334309fa9c2edfc208c3ddeb59731\n" +
                        "24689.tsv 50138bc29dd5936945432545663994d9\n" +
                        "24621.tsv 5909ef4c561e932a2bf0a0de261b5d49\n" +
                        "24620.tsv 19036cc59b62694bf099975d28802383\n" +
                        "24617.tsv 1a32921753c9e70f84a476aadeeee8b0\n" +
                        "24616.tsv c8c71a65e056776c64046d8c05b9732c\n" +
                        "24609.tsv c091a70f4706d79da6291dee7416ee2d\n" +
                        "24602.tsv bcd27c912210046e4a94c0cac3464b55\n" +
                        "-31640.tsv 1e389fe52b8cd97ade57f2b9ad7acf45\n" +
                        "-31636.tsv 18d503b033681f09c46bf5db8850452c\n" +
                        "-31635.tsv 44ab02d0f78e97ddbd1cc5d7cfa46e95",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "nodes".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName() + " " + fileMd5(file)).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("00001.tsv 765f1bbb404f7e913f5dcb80d7e76ffd\n" +
                        "00000.tsv 8f7fbb4a08e2cd73da32deb0c98c3224",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "relations".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName() + " " + fileMd5(file)).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("multipolygon_ae 6f02901fa57712d330b9a2fdbccdd5b2\n" +
                        "multipolygon_ad 0476d21a4d8d666335f4cc722be5975c\n" +
                        "multipolygon_ac 13414dd046f71eacf7b74d1a993df286\n" +
                        "multipolygon_ab 740363e80a8126a73f98d46224e16ef9\n" +
                        "multipolygon_aa c0d076cff30eb34e1fa5a977be3c085f",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "multipolygon".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName() + " " + fileMd5(file)).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));

        assertTrue(pbfFile.delete());
    }

    private static String fileMd5(File file) {
        try {
            byte[] hash = MessageDigest.getInstance("MD5").digest(Files.readAllBytes(file.toPath()));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
