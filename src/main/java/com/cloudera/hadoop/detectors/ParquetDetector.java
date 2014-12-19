package com.cloudera.hadoop.detectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;

public class ParquetDetector implements Detector {

    private static final byte[] MAGIC = new byte[] { 'P', 'A', 'R', '1' };

    private int version;

    @Override
    public String getName() {
        return "parquet";
    }

    @Override
    public boolean detect(byte[] header) {

        if (header.length >= MAGIC.length) {
            for (int i=0; i< MAGIC.length; i++) {
                if (MAGIC[i] != header[i]) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public String analyze(Configuration configuration, FileStatus fileStatus) throws IOException {
        ParquetMetadata meta = ParquetFileReader.readFooter(configuration, fileStatus);
        String metadataString = meta.toString();
        return "parquet file with " + meta.getBlocks().size() + " splits\n\n" + metadataString;
    }

}
