package com.github.isuhorukov.osm.pgsnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PipedProcess implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(PipedProcess.class);

    private final Process process;
    private final String commandDescription;
    private final Thread stderrLogger;

    private PipedProcess(Process process, String commandDescription) {
        this.process = process;
        this.commandDescription = commandDescription;
        this.stderrLogger = new Thread(this::logStderr, "piped-process-stderr");
        this.stderrLogger.setDaemon(true);
        this.stderrLogger.start();
    }

    public static PipedProcess start(List<String> command, File workingDirectory) throws IOException {
        String commandDescription = String.join(" ", command);
        log.info(commandDescription);
        return new PipedProcess(new ProcessBuilder(command).directory(workingDirectory).start(), commandDescription);
    }

    public InputStream getInputStream() {
        return process.getInputStream();
    }

    public void waitForSuccess() throws InterruptedException {
        int exitCode = process.waitFor();
        stderrLogger.join();
        if (exitCode != 0) {
            throw new IllegalStateException("exit code " + exitCode + " command: " + commandDescription);
        }
    }

    @Override
    public void close() {
        process.destroy();
    }

    private void logStderr() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.info(line);
            }
        } catch (IOException e) {
            log.warn("Failed to read stderr of command: {}", commandDescription, e);
        }
    }
}
