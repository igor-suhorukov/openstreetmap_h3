package com.github.isuhorukov.osm.pgsnapshot;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PipedProcessTest {

    @TempDir
    Path tempDir;

    @Test
    void getInputStream_readsProcessStdout() throws Exception {
        try (PipedProcess process = PipedProcess.start(List.of("echo", "osm"), tempDir.toFile())) {
            assertEquals("osm\n", IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8));
            assertDoesNotThrow(process::waitForSuccess);
        }
    }

    @Test
    void waitForSuccess_nonZeroExitCode_throws() throws Exception {
        try (PipedProcess process = PipedProcess.start(List.of("false"), tempDir.toFile())) {
            assertEquals(0, IOUtils.toByteArray(process.getInputStream()).length);

            IllegalStateException exception = assertThrows(IllegalStateException.class, process::waitForSuccess);
            assertTrue(exception.getMessage().startsWith("exit code 1 "));
        }
    }

    @Test
    void close_outputNotConsumed_stopsProcess() throws Exception {
        PipedProcess process = PipedProcess.start(List.of("sleep", "60"), tempDir.toFile());

        process.close();

        assertTimeoutPreemptively(Duration.ofSeconds(10),
                () -> assertThrows(IllegalStateException.class, process::waitForSuccess));
    }
}
