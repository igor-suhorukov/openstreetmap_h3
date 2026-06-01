package com.github.isuhorukov.osm.pgsnapshot;

import com.beust.jcommander.JCommander;
import com.github.isuhorukov.osm.pgsnapshot.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OsmPbfTransformation {

    private static final Logger log = LoggerFactory.getLogger(OsmPbfTransformation.class);

    public static final boolean IS_UDT_ENABLED = Boolean.getBoolean("udt");

    public static void main(String[] args) throws Exception {
        CliParameters parameters = parseCliArguments(args);
        if (parameters == null) {
            return;
        }
        if (!parameters.isSaveArrow() && !parameters.isSavePostgresqlTsv()) {
            throw new IllegalArgumentException("result_in_tsv or/and arrow_format parameters should be specified");
        }
        new Pipeline(parameters).run();
    }

    private static CliParameters parseCliArguments(String[] args) {
        CliParameters parameters = new CliParameters();
        JCommander jc = JCommander.newBuilder().addObject(parameters).build();
        try {
            jc.parse(args);
        } catch (Exception e) {
            log.warn(e.getMessage());
            jc.usage();
            return null;
        }
        if (parameters.isHelp()) {
            jc.usage();
            return null;
        }
        return parameters;
    }
}
