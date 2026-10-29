package com.github.isuhorukov.osm.pgsnapshot;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class OsmPbfTransformationTest {

    public static final String TEST_DATA_URL = "https://download.geofabrik.de/asia/maldives-250101.osm.pbf";

    @Test
    void dockerSmokeTest() throws Exception {
        assumeFalse(isOsmiumAvailable(), "osmium tool available, preferring localSmokeTest");
        try (GenericContainer<?> container = new GenericContainer<>(
                new ImageFromDockerfile("openstreetmap_h3:1.0", false)
                        .withFileFromPath("Dockerfile", Paths.get("../Dockerfile"))
                        .withFileFromPath("pom.xml", Paths.get("../pom.xml"))
                        .withFileFromPath("extension-api/pom.xml", Paths.get("../extension-api/pom.xml"))
                        .withFileFromPath("extension-api/src/main", Paths.get("../extension-api/src/main"))
                        .withFileFromPath("loader/pom.xml", Paths.get("pom.xml"))
                        .withFileFromPath("loader/src/main", Paths.get("src/main"))
                        .withFileFromPath("poi-organicmaps/pom.xml", Paths.get("../poi-organicmaps/pom.xml"))
                        .withFileFromPath("poi-organicmaps/src/main", Paths.get("../poi-organicmaps/src/main")))){

            String prefix = "maldives";
            File pbfFile = getFileForTest(prefix, TEST_DATA_URL);

            container.withWorkingDirectory(pbfFile.getParent());

            String containerPath = "/input";
            container.addFileSystemBind(pbfFile.getParent(), containerPath, BindMode.READ_WRITE, SelinuxContext.SHARED);

            container.setCommand("-source_pbf", containerPath+"/"+pbfFile.getName(), "-arrow_format", "PARQUET");
            container.start();
            container.execInContainer("chmod", "-R", "777", containerPath+"/"+prefix+"*");

            WaitingConsumer waitingConsumer = new WaitingConsumer();

            Consumer<OutputFrame> composedConsumer = new Slf4jLogConsumer(LoggerFactory.getLogger(this.getClass())).andThen(waitingConsumer);
            container.followOutput(composedConsumer);

            waitingConsumer.waitUntil(frame ->
                    frame.getUtf8String().contains("grep $'\\trelation\\t'"), 2, TimeUnit.MINUTES);

            assertOpenstreetmapH3Result(pbfFile);
            assertTrue(pbfFile.delete());
        }
    }

    @NotNull
    private static File getFileForTest(String prefix, String url) throws IOException {
        return download(Files.createTempFile(prefix, "240825.osm.pbf").toFile(), url);
    }

    @NotNull
    private static File download(File pbfFile, String url) throws IOException {
        try (InputStream in = URI.create(url).toURL().openStream();
             FileOutputStream out = new FileOutputStream(pbfFile)) {
            IOUtils.copy(in, out);
        }
        return pbfFile;
    }

    @Test
    void localSmokeTest(@TempDir Path tempDir) throws Exception {
        assumeTrue(isOsmiumAvailable(), "osmium tool not found in PATH");

        File pbfFile = download(tempDir.resolve("maldives-250101.osm.pbf").toFile(), TEST_DATA_URL);

        OsmPbfTransformation.main(new String[]{"-source_pbf", pbfFile.getAbsolutePath(),
                "-arrow_format", "PARQUET"});

        assertOpenstreetmapH3Result(pbfFile);
    }

    private static boolean isOsmiumAvailable() {
        try {
            return new ProcessBuilder("which", "osmium").start().waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static void assertOpenstreetmapH3Result(File pbfFile) {
        File[] files = new File(pbfFile.getParent(),
                pbfFile.getName().replace(".osm.pbf", "_loc_ways")).listFiles();

        Set<String> dir = Set.of("import_related_metadata", "multipolygon", "nodes", "relations", "sql", "sql_finalize", "static", "ways", "arrow");
        assertTrue(Arrays.stream(Objects.requireNonNull(files)).allMatch(file -> dir.contains(file.getName())));

        File arrowDir = Arrays.stream(files).filter(f -> "arrow".equals(f.getName())).findFirst().orElseThrow();
        File[] arrowEntries = Objects.requireNonNull(arrowDir.listFiles());
        Set<String> arrowEntryNames = Arrays.stream(arrowEntries).map(File::getName).collect(Collectors.toSet());
        assertTrue(arrowEntryNames.contains("nodes"));
        assertTrue(arrowEntryNames.contains("ways"));
        assertTrue(arrowEntryNames.contains("relations"));
        assertTrue(arrowEntryNames.contains("multipolygon"));
        assertTrue(Arrays.stream(Objects.requireNonNull(new File(arrowDir, "multipolygon").listFiles()))
                .anyMatch(f -> f.getName().endsWith(".parquet")));
        assertTrue(Arrays.stream(Objects.requireNonNull(new File(arrowDir, "nodes").listFiles()))
                .anyMatch(f -> f.getName().endsWith(".parquet")));
        assertTrue(Arrays.stream(Objects.requireNonNull(new File(arrowDir, "ways").listFiles()))
                .anyMatch(f -> f.getName().endsWith(".parquet")));
        assertTrue(Arrays.stream(Objects.requireNonNull(new File(arrowDir, "relations").listFiles()))
                .anyMatch(f -> f.getName().endsWith(".parquet")));
        assertParquetCount(23475, arrowDir + "/nodes/*.parquet");
        assertParquetCount(63017, arrowDir + "/ways/*.parquet");
        assertParquetCount(1160,  arrowDir + "/relations/*.parquet");
        assertParquetCount(1125,  arrowDir + "/multipolygon/*.parquet");
        assertEquals("32767.tsv 355191\n" +
                        "24942.tsv 331371\n" +
                        "24940.tsv 1685671\n" +
                        "24929.tsv 1153250\n" +
                        "24926.tsv 4605872\n" +
                        "24924.tsv 4705047\n" +
                        "24922.tsv 63204\n" +
                        "24920.tsv 3306\n" +
                        "24913.tsv 1149154\n" +
                        "24901.tsv 2957694\n" +
                        "24900.tsv 580\n" +
                        "24899.tsv 4924185\n" +
                        "24898.tsv 12592711\n" +
                        "24897.tsv 2648173\n" +
                        "24896.tsv 2295266\n" +
                        "24621.tsv 2503636\n" +
                        "-31636.tsv 1858680\n" +
                        "-31635.tsv 3187291",
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
        assertEquals("multipolygon_ae 202950\n" +
                        "multipolygon_ad 1071783\n" +
                        "multipolygon_ac 513831\n" +
                        "multipolygon_ab 779131\n" +
                        "multipolygon_aa 743140",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "multipolygon".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName()+" "+file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));

        assertEquals("32767.tsv 4ddf2716d80e456561cbe8f5bf4ab89d\n" +
                        "24942.tsv 647c0db88c0eca3ef52e579d4824c7ad\n" +
                        "24940.tsv 9d1bc7f60ca602c74a96337d0b580155\n" +
                        "24929.tsv cc18bf55558653c7b4d5a914ca65e6be\n" +
                        "24926.tsv be582fe615de7e739bb6b5ea1db2c250\n" +
                        "24924.tsv fd22a33e6b784ca4ab999f32d710568f\n" +
                        "24922.tsv 78e01e7d3d56f31446a6b6137713455b\n" +
                        "24920.tsv 7216570d23a0d79f08e0b54bdffaaa27\n" +
                        "24913.tsv 51a60b6fd9f1eb7ac18ed314fd9037d2\n" +
                        "24901.tsv 13647ccf86ae4b2607c844a8f8f5a515\n" +
                        "24900.tsv ad7071ee628d88ee352fc521290f95db\n" +
                        "24899.tsv ec069f3fd615a38f381fa187c3eefee2\n" +
                        "24898.tsv 000b2dcf6dece1bf1c144c2e3fcc2c8d\n" +
                        "24897.tsv 4aa0047fba5707fd8006cb7e5409bedc\n" +
                        "24896.tsv a66fb0f7400eb12456eeccd27fabdc99\n" +
                        "24621.tsv 3b501ae3e4232d311e0d897c094d885a\n" +
                        "-31636.tsv 07a993b9818ae4abe62db55b80996422\n" +
                        "-31635.tsv 997ee92f05a6da681ee3b177e97b7840",
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
        assertEquals("multipolygon_ae 72b87149084db0e73ecfc56a2897e4cd\n" +
                        "multipolygon_ad dbff1012396b46040a6337b5fbdb3aaa\n" +
                        "multipolygon_ac c7804feff81fa80a954c9321953966e3\n" +
                        "multipolygon_ab 604e754d79a96abde4a74f7e86f0b53c\n" +
                        "multipolygon_aa 17bdbba9e2e7aea63210c4443f3dc2aa",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "multipolygon".contains(file.getName())).
                                        findFirst().orElse(pbfFile).listFiles())).
                        map(file -> file.getName() + " " + fileMd5(file)).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        File metadataDir = Arrays.stream(files).filter(file -> "import_related_metadata".equals(file.getName())).
                findFirst().orElseThrow();
        File blockContentFile = new File(metadataDir, "osm_file_block_content.tsv");
        assertEquals(9983, blockContentFile.length());
        assertEquals("afb3fb78fc7d814afcdfe525d4e0c259", fileMd5(blockContentFile));
        assertEquals("y_multipoly_ae.sql 82\n" +
                        "y_multipoly_ad.sql 82\n" +
                        "y_multipoly_ac.sql 82\n" +
                        "y_multipoly_ab.sql 82\n" +
                        "y_multipoly_aa.sql 82\n" +
                        "ways_import_003.sql 631\n" +
                        "ways_import_002.sql 839\n" +
                        "ways_import_001.sql 1451\n" +
                        "ways_import_000.sql 1451\n" +
                        "ways_32767_import.sql 1606\n" +
                        "relations_import.sql 546\n" +
                        "nodes_import_003.sql 587\n" +
                        "nodes_import_002.sql 1791\n" +
                        "nodes_import_001.sql 1187\n" +
                        "nodes_import_000.sql 827",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "sql".equals(file.getName())).
                                        findFirst().orElseThrow().listFiles())).
                        map(file -> file.getName() + " " + file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("osmium_export.json 134\n" +
                        "multipolygon_tables.sql 385\n" +
                        "multipolygon_insert.sql 1500\n" +
                        "multipolygon.sql 456\n" +
                        "h3_poly.tsv.gz 3315290\n" +
                        "database_init.sql 12254\n" +
                        "database_after_init.sql 4876",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "static".equals(file.getName())).
                                        findFirst().orElseThrow().listFiles())).
                        map(file -> file.getName() + " " + file.length()).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("osmium_export.json c130cbc588a91ee414184f966eb77726\n" +
                        "multipolygon_tables.sql 8dbbadafcf974eb26456198d118ba3c3\n" +
                        "multipolygon_insert.sql 5259dbea7e04b6e7b47838f913475e22\n" +
                        "multipolygon.sql 4c88c2c2f57396ccf0609e3eefb59708\n" +
                        "h3_poly.tsv.gz 680d35581dc024ff1006b22067a96c89\n" +
                        "database_init.sql 14c8c0bdae5da333aa6d878beb421bf6\n" +
                        "database_after_init.sql 5f2ffc1ccbdd46fe8869b4012a5828a9",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "static".equals(file.getName())).
                                        findFirst().orElseThrow().listFiles())).
                        map(file -> file.getName() + " " + fileMd5(file)).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("y_multipoly_ae.sql 69b19e380ae4fb5eca7c422d8de3066e\n" +
                        "y_multipoly_ad.sql 7e2e175b59f5aa2f84dc59bd63fbdc7b\n" +
                        "y_multipoly_ac.sql 1b66980ed4282ca99fb058a9b830bfb5\n" +
                        "y_multipoly_ab.sql be4e03275a3a8a39b68f90ada6778c87\n" +
                        "y_multipoly_aa.sql dc6e8a5c07a2914d0690879beb96eb25\n" +
                        "ways_import_003.sql db45216f961051397b10cb2a268ce3de\n" +
                        "ways_import_002.sql 5f39f64474a38cd26547991da66123c6\n" +
                        "ways_import_001.sql 584400fbea0292151fc6864846f1ef59\n" +
                        "ways_import_000.sql 927732ed931763ef7ea1899ea42742eb\n" +
                        "ways_32767_import.sql 70453ad3f7f0a5c34b5b628a8ce4509d\n" +
                        "relations_import.sql b01daaf3a801bd3e7fb1dac67c3a4294\n" +
                        "nodes_import_003.sql 87d9bf3359cf3bad460483265677c1b0\n" +
                        "nodes_import_002.sql d88648c3b1d7187717dde03e992621d2\n" +
                        "nodes_import_001.sql fa777586acd1cba90973d76fe4d6b07a\n" +
                        "nodes_import_000.sql e42390d31f4377663ed604938faa9b84",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "sql".equals(file.getName())).
                                        findFirst().orElseThrow().listFiles())).
                        map(file -> file.getName() + " " + fileMd5(file)).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
        assertEquals("ways_default_partition.sql 092c85628d8ca4beea6adce393bf7ba8\n" +
                        "nodes_analyze.sql 127293d7bee38258b8d04a4d83b17365\n" +
                        "multipolygon_import.sql 2b81dceb7d17a0156a1f6a0539b9a90f",
                Arrays.stream(Objects.requireNonNull(
                                Arrays.stream(files).filter(file -> "sql_finalize".equals(file.getName())).
                                        findFirst().orElseThrow().listFiles())).
                        map(file -> file.getName() + " " + fileMd5(file)).
                        sorted(Comparator.reverseOrder()).
                        collect(Collectors.joining("\n")));
    }

    private static void assertParquetCount(long expected, String globPath) {
        try (java.sql.Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM '" + globPath + "'")) {
            rs.next();
            assertEquals(expected, rs.getLong(1), "row count mismatch for " + globPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
