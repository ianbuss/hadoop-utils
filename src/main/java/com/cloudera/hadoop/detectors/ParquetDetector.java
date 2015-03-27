package com.cloudera.hadoop.detectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;

public class ParquetDetector extends AbstractDetector {

    public ParquetDetector() {
        magic = new byte[] { 'P', 'A', 'R', '1' };
    }

    @Override
    public String getName() {
        return "parquet";
    }

    @Override
    public String analyze(Configuration configuration, FileStatus fileStatus) throws IOException {
        ParquetMetadata meta = ParquetFileReader.readFooter(configuration, fileStatus);
        String metadataString = meta.toString();
        return "Parquet file with " + meta.getBlocks().size() + " splits\n\n" + metadataString;
    }

}
