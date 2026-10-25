package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class StreamedScript {

    private final List<PsqlCommand> prefix;
    private final Map<String, String> copySqlByInputPath;
    private final List<PsqlCommand> suffix;

    private StreamedScript(List<PsqlCommand> prefix, Map<String, String> copySqlByInputPath, List<PsqlCommand> suffix) {
        this.prefix = prefix;
        this.copySqlByInputPath = Collections.unmodifiableMap(copySqlByInputPath);
        this.suffix = suffix;
    }

    static StreamedScript of(String script) {
        List<PsqlCommand> commands = PsqlScript.parse(script).afterLastConnect();
        Map<String, String> copySqlByInputPath = new LinkedHashMap<>();
        int first = -1;
        int last = -1;
        for (int i = 0; i < commands.size(); i++) {
            PsqlCommand command = commands.get(i);
            CopyStatement copy = command.isMeta() ? null : CopyStatement.parse(command.getSql());
            if (copy != null && !copy.isGzip()) {
                if (last >= 0 && last != i - 1) {
                    throw new IllegalArgumentException("Only COPY commands are allowed between streamed COPY commands: " + command);
                }
                if (first < 0) {
                    first = i;
                }
                last = i;
                copySqlByInputPath.put(copy.getInputPath(), copy.getSql());
            }
        }
        if (first < 0) {
            throw new IllegalArgumentException("Script has no COPY from " + InputFiles.INPUT_DIRECTORY);
        }
        return new StreamedScript(commands.subList(0, first), copySqlByInputPath, commands.subList(last + 1, commands.size()));
    }

    List<PsqlCommand> getPrefix() {
        return prefix;
    }

    List<PsqlCommand> getSuffix() {
        return suffix;
    }

    Map<String, String> getCopySqlByInputPath() {
        return copySqlByInputPath;
    }
}
