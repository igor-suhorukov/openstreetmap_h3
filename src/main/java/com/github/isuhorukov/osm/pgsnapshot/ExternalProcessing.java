package com.github.isuhorukov.osm.pgsnapshot;

import com.github.isuhorukov.osm.pgsnapshot.model.statistics.MultipolygonTime;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ExternalProcessing {
    public static final String MULTIPOLYGON_SOURCE_TSV = "/multipolygon/source.tsv";

    public static MultipolygonTime prepareMultipolygonDataAndScripts(File sourcePbfFile, File resultDirectory,
                                                                     int scriptCount, long multipolygonCount)
            throws IOException, InterruptedException {

        MultipolygonTime multipolygonTime = new MultipolygonTime();

        String resultDirName = resultDirectory.getName();
        String basePath = resultDirectory.getParent();
        String indexType = getIndexType(sourcePbfFile);
        String multipolygonCommand = "docker run -w /wkd -v "+basePath+":/wkd mschilde/osmium-tool osmium export  -e --config=" + resultDirName + "/static/osmium_export.json --fsync -i "+indexType+" --geometry-types polygon  -v -f pg -x tags_type=hstore " + sourcePbfFile.getName() + " -o " + (resultDirName + MULTIPOLYGON_SOURCE_TSV);
        long osmiumExportStart = System.currentTimeMillis();
        runCliCommand(multipolygonCommand, basePath);
        multipolygonTime.setMultipolygonExportTime(System.currentTimeMillis()-osmiumExportStart);

        long partSize = multipolygonCount / scriptCount +1;
        String resultDirFullPath = basePath + "/" + resultDirName;
        String multipolygonSourceTsv = resultDirFullPath + MULTIPOLYGON_SOURCE_TSV;
        String splitCommand = "cat "+multipolygonSourceTsv+" | grep $'\\trelation\\t' | split -l " + partSize + " - " + resultDirFullPath + "/multipolygon/multipolygon_";
        System.out.println(splitCommand);
        long multipolygonSplitStart = System.currentTimeMillis();
        runCliCommand(new String[]{"/bin/bash", "-c", splitCommand});
        multipolygonTime.setSplitMultipolygonByPartsTime(System.currentTimeMillis()-multipolygonSplitStart);

        new File(multipolygonSourceTsv).delete();

        generateMultipolygonCopyScripts(resultDirFullPath);
        return multipolygonTime;
    }

    static void generateMultipolygonCopyScripts(String resultDirFullPath) throws IOException {
        List<String> multipolyParts = Arrays.stream(
                Objects.requireNonNull(new File(resultDirFullPath, "multipolygon").listFiles())).
                map(File::getName).collect(Collectors.toList());
        for(String multipolyPart: multipolyParts){
            String copyCommand = "\\timing on\ncopy preimport_multipolygon FROM '/input/multipolygon/" + multipolyPart + "';";
            try (FileOutputStream copyScriptOutput = new FileOutputStream(String.format("%s/sql/y_multipoly_%s.sql",
                    resultDirFullPath, multipolyPart.replace("multipolygon_", "")));){
                IOUtils.write(copyCommand, copyScriptOutput, StandardCharsets.UTF_8);
            }
        }
    }

    public static Splitter.Blocks enrichSourcePbfAndSplitIt(File sourcePbfFile, boolean preserveAllNodes)
            throws IOException, InterruptedException {
        String basePath = sourcePbfFile.getParent();
        String sourcePbfName= sourcePbfFile.getName();
        String resultPbfName = sourcePbfName.replace(".osm","").replace(".pbf","_loc_ways.pbf");
        File resultPbfNameFile = new File(basePath, resultPbfName);
        File blockDirectory = new File(Splitter.getBlockDirectoryName(resultPbfNameFile));
        if(blockDirectory.exists()){
            return new Splitter.Blocks(blockDirectory.getAbsolutePath(), -1, -1 ,-1);
        }

        String indexType = getIndexType(sourcePbfFile);

        long addLocationStart = System.currentTimeMillis();
        if(!resultPbfNameFile.exists()){
            String enrichCommand="docker run -w /wkd -v "+basePath+":/wkd mschilde/osmium-tool osmium add-locations-to-ways " +
                    sourcePbfName + " -v --output-format pbf,pbf_compression=none "+
                    (preserveAllNodes ? "--keep-untagged-nodes" : "--keep-member-nodes")+
                    " -i " + indexType + " -o " + resultPbfName;
            runCliCommand(enrichCommand, basePath);
        }

        return Splitter.splitPbfByBlocks(new File(basePath, resultPbfName),
                            System.currentTimeMillis() - addLocationStart);
    }


    private static void runCliCommand(String[] cmdarray) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(cmdarray);
        int exitCode = process.waitFor();
        System.out.println(IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8));
        System.out.println(IOUtils.toString(process.getErrorStream(), StandardCharsets.UTF_8));
        if(exitCode !=0){
            throw new RuntimeException("exit code "+exitCode+" command: "+ Arrays.toString(cmdarray));
        }
    }

    private static String getIndexType(File sourcePbfFile) {
        long sourceFileLength = sourcePbfFile.length();
        long maxMemory = Runtime.getRuntime().maxMemory();

        return sourceFileLength>maxMemory ? "dense_file_array" : "sparse_mem_array";
    }

    private static void runCliCommand(String command, String basePath) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command, new String[0], new File(basePath).getParentFile());
        int exitCode = process.waitFor();
        System.out.println(IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8));
        System.out.println(IOUtils.toString(process.getErrorStream(), StandardCharsets.UTF_8));
        if(exitCode !=0){
            throw new RuntimeException("exit code "+exitCode+" command: "+ command);
        }
    }
}
