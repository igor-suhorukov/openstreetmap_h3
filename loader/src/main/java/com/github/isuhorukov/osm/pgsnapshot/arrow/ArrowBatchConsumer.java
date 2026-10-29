package com.github.isuhorukov.osm.pgsnapshot.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

@FunctionalInterface
public interface ArrowBatchConsumer {

    void accept(VectorSchemaRoot batch) throws IOException;
}
