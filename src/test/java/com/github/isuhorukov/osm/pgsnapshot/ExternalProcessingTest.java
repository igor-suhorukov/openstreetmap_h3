package com.github.isuhorukov.osm.pgsnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ExternalProcessingTest {

    @TempDir
    Path tempDir;

    @Test
    void getIndexType_fileSmallEnough_returnsSparseMemArray() throws IOException {
        File smallFile = Files.createTempFile(tempDir, "small", ".pbf").toFile();
        new FileOutputStream(smallFile).close();

        assertEquals("sparse_mem_array", ExternalProcessing.getIndexType(smallFile));
    }

    @Test
    void getIndexType_fileLargerThanMaxMemory_returnsDenseFileArray() throws IOException {
        File largeFile = new File(tempDir.toFile(), "large.pbf") {
            @Override
            public long length() {
                return Runtime.getRuntime().maxMemory() + 1;
            }
        };

        assertEquals("dense_file_array", ExternalProcessing.getIndexType(largeFile));
    }

    @Test
    void enrichSourcePbfAndSplitIt_blockDirAlreadyExists_returnsEarlyWithNegativeTimes() throws IOException, InterruptedException {
        // Create a fake source pbf file
        File sourceDir = tempDir.toFile();
        File sourcePbf = new File(sourceDir, "test.osm.pbf");
        sourcePbf.createNewFile();

        // Pre-create the block directory that would normally be produced
        File resultPbf = new File(sourceDir, "test_loc_ways.pbf");
        String blockDirName = Splitter.getBlockDirectoryName(resultPbf);
        new File(blockDirName).mkdirs();

        ExternalProcessing ep = new ExternalProcessing(false);
        Splitter.Blocks blocks = ep.enrichSourcePbfAndSplitIt(sourcePbf, false);

        assertEquals(blockDirName, blocks.getDirectory());
        assertEquals(-1, blocks.getBlobCount());
        assertEquals(-1, blocks.getPbfSplitTime());
        assertEquals(-1, blocks.getAddLocationsToWaysTime());
    }

    @Test
    void generateMultipolygonCopyScripts_createsOneSqlFilePerPart() throws IOException {
        File resultDir = tempDir.toFile();
        File multipolygonDir = new File(resultDir, "multipolygon");
        multipolygonDir.mkdirs();
        File sqlDir = new File(resultDir, "sql");
        sqlDir.mkdirs();

        new File(multipolygonDir, "multipolygon_aa").createNewFile();
        new File(multipolygonDir, "multipolygon_ab").createNewFile();

        ExternalProcessing.generateMultipolygonCopyScripts(resultDir.getAbsolutePath());

        File[] sqlFiles = sqlDir.listFiles((d, name) -> name.startsWith("y_multipoly_"));
        assertNotNull(sqlFiles);
        assertEquals(2, sqlFiles.length);
        for (File sqlFile : sqlFiles) {
            String content = Files.readString(sqlFile.toPath());
            assertTrue(content.contains("copy preimport_multipolygon FROM"));
            assertTrue(content.contains("\\timing on"));
        }
    }

    @Test
    void generateMultipolygonCopyScripts_sqlContentIsCorrect() throws IOException {
        File resultDir = tempDir.toFile();
        new File(resultDir, "multipolygon").mkdirs();
        new File(resultDir, "sql").mkdirs();
        new File(resultDir, "multipolygon/multipolygon_aa").createNewFile();

        ExternalProcessing.generateMultipolygonCopyScripts(resultDir.getAbsolutePath());

        File script = new File(resultDir, "sql/y_multipoly_aa.sql");
        assertTrue(script.exists());
        String content = Files.readString(script.toPath());
        assertTrue(content.contains("/input/multipolygon/multipolygon_aa"));
    }
}
