package com.cloudera.hadoop.detectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.orc.Metadata;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class ORCFileDetector implements Detector {

    private static final byte[] MAGIC = new byte[] { 'O', 'R', 'C' };

    @Override
    public String getName() {
        return "ORCFile";
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

        FileSystem fs = FileSystem.get(configuration);
        Reader reader = OrcFile.createReader(fs, fileStatus.getPath());
        Metadata metadata = reader.getMetadata();
        String compressionType = reader.getCompression().toString();

        int blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;

        return "ORCFile with " +
                blocks + (blocks == 1 ? " block" : " blocks") + "\n\n" +
                "compression: " + compressionType;
    }
}
