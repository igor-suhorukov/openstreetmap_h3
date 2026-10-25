package com.github.isuhorukov.osm.pgsnapshot.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CopyStatementTest {

    @Test
    void parse_fileSource_rewrittenToStdin() {
        CopyStatement copy = CopyStatement.parse("COPY relations(id,tags) FROM '/input/relations/00000.tsv' "
                + "DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV FREEZE");

        assertNotNull(copy);
        assertEquals("COPY relations(id,tags) FROM STDIN DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV FREEZE", copy.getSql());
        assertEquals("/input/relations/00000.tsv", copy.getInputPath());
        assertFalse(copy.isGzip());
    }

    @Test
    void parse_lowerCaseMultiline_rewritten() {
        CopyStatement copy = CopyStatement.parse("copy osm_file_block(id,\n nodeCount) from\n '/input/import_related_metadata/osm_file_block.tsv'");

        assertNotNull(copy);
        assertEquals("copy osm_file_block(id,\n nodeCount) from\n STDIN", copy.getSql());
        assertEquals("/input/import_related_metadata/osm_file_block.tsv", copy.getInputPath());
    }

    @Test
    void parse_zcatProgram_gzipSource() {
        CopyStatement copy = CopyStatement.parse("copy h3_3_bounds_complex(id,h3_0,h3_1,h3_2,bbox,bounds) from PROGRAM "
                + "$$ zcat /input/static/h3_poly.tsv.gz$$ DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV");

        assertNotNull(copy);
        assertEquals("copy h3_3_bounds_complex(id,h3_0,h3_1,h3_2,bbox,bounds) from STDIN DELIMITER E'\\t' ESCAPE '\\' NULL '\\N' CSV",
                copy.getSql());
        assertEquals("/input/static/h3_poly.tsv.gz", copy.getInputPath());
        assertTrue(copy.isGzip());
    }

    @Test
    void parse_otherStatements_null() {
        assertNull(CopyStatement.parse("SELECT 1"));
        assertNull(CopyStatement.parse("COPY t FROM STDIN"));
        assertNull(CopyStatement.parse("COPY t FROM '/tmp/data.tsv'"));
        assertNull(CopyStatement.parse("COPY t TO '/input/data.tsv'"));
        assertNull(CopyStatement.parse("COPY t FROM PROGRAM 'cat /input/data.tsv'"));
    }
}
