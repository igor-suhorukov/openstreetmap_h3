package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class PsqlScript {

    private final List<PsqlCommand> commands;

    private PsqlScript(List<PsqlCommand> commands) {
        this.commands = Collections.unmodifiableList(commands);
    }

    static PsqlScript parse(String text) {
        return new Parser(text).parse();
    }

    List<PsqlCommand> getCommands() {
        return commands;
    }

    List<PsqlCommand> afterLastConnect() {
        for (int i = commands.size() - 1; i >= 0; i--) {
            PsqlCommand command = commands.get(i);
            if (command.isMeta() && isConnect(command.getMetaCommand())) {
                return commands.subList(i + 1, commands.size());
            }
        }
        return commands;
    }

    static boolean isConnect(String metaCommand) {
        return "c".equals(metaCommand) || "connect".equals(metaCommand);
    }

    private static final class Parser {
        private final String text;
        private final List<PsqlCommand> commands = new ArrayList<>();
        private final StringBuilder statement = new StringBuilder();
        private int position;

        private Parser(String text) {
            this.text = text;
        }

        private PsqlScript parse() {
            while (position < text.length()) {
                parseNext(text.charAt(position));
            }
            addStatement(false);
            return new PsqlScript(commands);
        }

        private void parseNext(char c) {
            String dollarTag = c == '$' ? dollarTag() : null;
            if (c == '\'') {
                appendUntil(quotedEnd('\'', isEscapeString()));
            } else if (c == '"') {
                appendUntil(quotedEnd('"', false));
            } else if (dollarTag != null) {
                appendUntil(dollarQuotedEnd(dollarTag));
            } else if (text.startsWith("--", position)) {
                skipLineComment();
            } else if (text.startsWith("/*", position)) {
                skipBlockComment();
            } else if (c == ';') {
                position++;
                addStatement(false);
            } else if (c == '\\') {
                metaCommand();
            } else {
                statement.append(c);
                position++;
            }
        }

        private void metaCommand() {
            int lineEnd = text.indexOf('\n', position);
            if (lineEnd < 0) {
                lineEnd = text.length();
            }
            String line = text.substring(position + 1, lineEnd).trim();
            position = lineEnd;
            String[] nameAndArgument = line.split("\\s+", 2);
            String name = nameAndArgument[0];
            String argument = nameAndArgument.length > 1 ? nameAndArgument[1].trim() : "";
            if (statement.toString().trim().isEmpty()) {
                statement.setLength(0);
                commands.add(PsqlCommand.meta(name, argument));
            } else if (("g".equals(name) || "gset".equals(name)) && argument.isEmpty()) {
                addStatement("gset".equals(name));
            } else {
                throw new IllegalArgumentException("psql meta-command \\" + line + " inside an SQL statement is not supported");
            }
        }

        private boolean isEscapeString() {
            if (position == 0) {
                return false;
            }
            char previous = text.charAt(position - 1);
            return (previous == 'E' || previous == 'e') && (position < 2 || !isIdentifierPart(text.charAt(position - 2)));
        }

        private int quotedEnd(char quote, boolean backslashEscapes) {
            int i = position + 1;
            while (i < text.length()) {
                char c = text.charAt(i);
                if (backslashEscapes && c == '\\') {
                    i += 2;
                } else if (c != quote) {
                    i++;
                } else if (i + 1 < text.length() && text.charAt(i + 1) == quote) {
                    i += 2;
                } else {
                    return i + 1;
                }
            }
            throw new IllegalArgumentException("Unterminated quoted text at offset " + position);
        }

        private String dollarTag() {
            if (position > 0 && isIdentifierPart(text.charAt(position - 1))) {
                return null;
            }
            int i = position + 1;
            if (i < text.length() && text.charAt(i) == '$') {
                return "$$";
            }
            if (i >= text.length() || !(Character.isLetter(text.charAt(i)) || text.charAt(i) == '_')) {
                return null;
            }
            while (i < text.length() && (Character.isLetterOrDigit(text.charAt(i)) || text.charAt(i) == '_')) {
                i++;
            }
            return i < text.length() && text.charAt(i) == '$' ? text.substring(position, i + 1) : null;
        }

        private int dollarQuotedEnd(String tag) {
            int end = text.indexOf(tag, position + tag.length());
            if (end < 0) {
                throw new IllegalArgumentException("Unterminated dollar-quoted text at offset " + position);
            }
            return end + tag.length();
        }

        private void skipLineComment() {
            int lineEnd = text.indexOf('\n', position);
            position = lineEnd < 0 ? text.length() : lineEnd;
        }

        private void skipBlockComment() {
            int depth = 0;
            int i = position;
            while (i < text.length()) {
                if (text.startsWith("/*", i)) {
                    depth++;
                    i += 2;
                } else if (text.startsWith("*/", i)) {
                    depth--;
                    i += 2;
                    if (depth == 0) {
                        position = i;
                        statement.append(' ');
                        return;
                    }
                } else {
                    i++;
                }
            }
            throw new IllegalArgumentException("Unterminated comment at offset " + position);
        }

        private void appendUntil(int end) {
            statement.append(text, position, end);
            position = end;
        }

        private void addStatement(boolean gset) {
            String sql = statement.toString().trim();
            statement.setLength(0);
            if (!sql.isEmpty()) {
                commands.add(PsqlCommand.sql(sql, gset));
            }
        }

        private static boolean isIdentifierPart(char c) {
            return Character.isLetterOrDigit(c) || c == '_' || c == '$';
        }
    }
}
