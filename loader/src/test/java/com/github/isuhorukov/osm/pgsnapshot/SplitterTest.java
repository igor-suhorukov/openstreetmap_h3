package com.github.isuhorukov.osm.pgsnapshot;

import org.junit.jupiter.api.Test;
import org.openstreetmap.osmosis.osmbinary.Fileformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SplitterTest {

    @Test
    void splitPbfByBlocks_passesDataBlobsInStreamOrder() throws IOException {
        ByteArrayOutputStream pbf = new ByteArrayOutputStream();
        writeBlob(pbf, "OSMHeader", "header");
        writeBlob(pbf, "OSMData", "nodes");
        writeBlob(pbf, "OSMData", "ways");

        List<String> blocks = new ArrayList<>();
        int blobCount = Splitter.splitPbfByBlocks(new ByteArrayInputStream(pbf.toByteArray()),
                (rawBlob, blockNumber) -> blocks.add(blockNumber + ":"
                        + new String(rawBlob.getData(), StandardCharsets.UTF_8)));

        assertEquals(2, blobCount);
        assertEquals(List.of("0:nodes", "1:ways"), blocks);
    }

    @Test
    void splitPbfByBlocks_emptyStream_noBlocks() {
        List<Long> blockNumbers = new ArrayList<>();

        int blobCount = Splitter.splitPbfByBlocks(new ByteArrayInputStream(new byte[0]),
                (rawBlob, blockNumber) -> blockNumbers.add(blockNumber));

        assertEquals(0, blobCount);
        assertTrue(blockNumbers.isEmpty());
    }

    private static void writeBlob(ByteArrayOutputStream pbf, String type, String data) throws IOException {
        byte[] blob = data.getBytes(StandardCharsets.UTF_8);
        byte[] header = Fileformat.BlobHeader.newBuilder().setType(type).setDatasize(blob.length).build().toByteArray();
        DataOutputStream output = new DataOutputStream(pbf);
        output.writeInt(header.length);
        output.write(header);
        output.write(blob);
        output.flush();
    }
}
