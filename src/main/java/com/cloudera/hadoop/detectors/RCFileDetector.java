package com.cloudera.hadoop.detectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class RCFileDetector extends AbstractDetector {

    private int version;

    public RCFileDetector() {
        magic = new byte[] { 'R', 'C', 'F' };
    }

    @Override
    public String getName() {
        return "RCFile";
    }

    @Override
    public boolean detect(byte[] header) {

        if (!super.detect(header)) {
            return false;
        }

        if (header.length >= 4) {
            version = (int) header[3];
        }

        return true;
    }

    @Override
    public String analyze(Configuration configuration, FileStatus fileStatus) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        RCFile.Reader reader = new RCFile.Reader(fs, fileStatus.getPath(), configuration);
        SequenceFile.Metadata metadata = reader.getMetadata();
        int blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;

        return "RCFile (version " + version + ") with " +
                blocks + (blocks == 1 ? " block" : " blocks") + "\n\n" +
                metadata.toString();
    }
}
