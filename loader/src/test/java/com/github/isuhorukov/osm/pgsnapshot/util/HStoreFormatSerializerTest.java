package com.github.isuhorukov.osm.pgsnapshot.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HStoreFormatSerializerTest {

    @Test
    void renderTags_null_appendsNullString() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.renderTags(null, sb);
        assertEquals("\\N", sb.toString());
    }

    @Test
    void renderTags_empty_appendsNullString() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.renderTags(Collections.emptyList(), sb);
        assertEquals("\\N", sb.toString());
    }

    @Test
    void renderTags_singleTag_formatsCorrectly() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.renderTags(List.of(new Tag("name", "value")), sb);
        assertEquals("\"\\\"name\\\"=>\\\"value\\\"\"", sb.toString());
    }

    @Test
    void renderTags_multipleTags_separatedByComma() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.renderTags(List.of(new Tag("k1", "v1"), new Tag("k2", "v2")), sb);
        String result = sb.toString();
        // outer quotes wrap it, two entries separated by comma
        assertEquals("\"\\\"k1\\\"=>\\\"v1\\\",\\\"k2\\\"=>\\\"v2\\\"\"", result);
    }

    @Test
    void renderTags_nullValue_writesNULL() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.renderTags(List.of(new Tag("key", null)), sb);
        // null value renders as NULL (unquoted)
        assertEquals("\"\\\"key\\\"=>NULL\"", sb.toString());
    }

    @Test
    void renderTags_specialCharsInKey_escapedCorrectly() {
        StringBuilder sb = new StringBuilder();
        // backslash, quote, newline, tab, CR, backspace, form-feed, vertical-tab
        HStoreFormatSerializer.renderTags(List.of(new Tag("\\\"\n\t\r\b\f\013", "v")), sb);
        String expected = "\"\\\"" + "\\\\\\\\" + "\\\\\\\"" + "\\n\\t\\r\\b\\f\\v" + "\\\"=>\\\"v\\\"\"";
        assertEquals(expected, sb.toString());
    }

    static Stream<Arguments> escapeStringCases() {
        return Stream.of(
                Arguments.of(null, "\\N"),
                Arguments.of("", "\\N"),
                Arguments.of("hello", "\"hello\""),
                Arguments.of("a\\b", "\"a\\\\b\""),
                Arguments.of("a\"b", "\"a\\\"b\""),
                Arguments.of("a\nb", "\"a\\nb\""),
                Arguments.of("a\tb", "\"a\\tb\""),
                Arguments.of("a\rb", "\"a\\rb\""),
                Arguments.of("a\bb", "\"a\\bb\""),
                Arguments.of("a\fb", "\"a\\fb\""),
                Arguments.of("a\013b", "\"a\\vb\""));
    }

    @ParameterizedTest
    @MethodSource("escapeStringCases")
    void escapeString_escapesSpecialCharacters(String value, String expected) {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, value);
        assertEquals(expected, sb.toString());
    }
}
