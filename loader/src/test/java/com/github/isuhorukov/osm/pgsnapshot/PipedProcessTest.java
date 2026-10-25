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
    void waitForSuccessUpTo_allowedExitCode_acceptedHigherRejected() throws Exception {
        try (PipedProcess partial = PipedProcess.start(List.of("sh", "-c", "exit 1"), tempDir.toFile());
             PipedProcess failed = PipedProcess.start(List.of("sh", "-c", "exit 2"), tempDir.toFile())) {
            assertDoesNotThrow(() -> partial.waitForSuccessUpTo(1));
            IllegalStateException exception = assertThrows(IllegalStateException.class, () -> failed.waitForSuccessUpTo(1));
            assertTrue(exception.getMessage().startsWith("exit code 2 "));
        }
    }

    @Test
    void start_standardInput_writtenAndClosed() throws Exception {
        try (PipedProcess process = PipedProcess.start(List.of("cat"), tempDir.toFile(),
                "{\"linear_tags\": false}".getBytes(StandardCharsets.UTF_8))) {
            assertEquals("{\"linear_tags\": false}", IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8));
            assertDoesNotThrow(process::waitForSuccess);
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
