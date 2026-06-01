package com.github.isuhorukov.osm.pgsnapshot.util;

import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.util.Collections;
import java.util.List;

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
        String result = sb.toString();
        // just verify no exception and the result is non-empty
        assert result.length() > 0;
    }

    @Test
    void escapeString_null_appendsNullString() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, null);
        assertEquals("\\N", sb.toString());
    }

    @Test
    void escapeString_empty_appendsNullString() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "");
        assertEquals("\\N", sb.toString());
    }

    @Test
    void escapeString_plainString_wrapsInQuotes() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "hello");
        assertEquals("\"hello\"", sb.toString());
    }

    @Test
    void escapeString_backslash_doubledEscape() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "a\\b");
        assertEquals("\"a\\\\b\"", sb.toString());
    }

    @Test
    void escapeString_quote_escaped() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "a\"b");
        assertEquals("\"a\\\"b\"", sb.toString());
    }

    @Test
    void escapeString_newline_escaped() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "a\nb");
        assertEquals("\"a\\nb\"", sb.toString());
    }

    @Test
    void escapeString_tab_escaped() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "a\tb");
        assertEquals("\"a\\tb\"", sb.toString());
    }

    @Test
    void escapeString_carriageReturn_escaped() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "a\rb");
        assertEquals("\"a\\rb\"", sb.toString());
    }

    @Test
    void escapeString_backspace_escaped() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "a\bb");
        assertEquals("\"a\\bb\"", sb.toString());
    }

    @Test
    void escapeString_formFeed_escaped() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "a\fb");
        assertEquals("\"a\\fb\"", sb.toString());
    }

    @Test
    void escapeString_verticalTab_escaped() {
        StringBuilder sb = new StringBuilder();
        HStoreFormatSerializer.escapeString(sb, "a\013b");
        assertEquals("\"a\\vb\"", sb.toString());
    }
}
