package com.cloudera.hadoop.detectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class SequenceFileDetector implements Detector {

    private static final byte[] MAGIC = new byte[] { 'S', 'E', 'Q' };

    private int version;

    @Override
    public String getName() {
        return "SequenceFile";
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
        if (header.length >= 4) {
            version = (int) header[3];
        }

        return true;
    }

    @Override
    public String analyze(Configuration configuration, FileStatus fileStatus) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, fileStatus.getPath(), configuration);
        String codec = reader.getCompressionCodec().getClass().getName();
        String compType = reader.getCompressionType().toString();
        String key = reader.getKeyClassName();
        String val = reader.getValueClassName();
        int blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;

        return "SequenceFile (version " + version + ") with " +
                blocks + (blocks == 1 ? "block" : "blocks") + "\n\n" +
                "Key: " + key + "\n" +
                "Value: " + val + "\n" +
                "Compression Type: " + compType + "\n"+
                "Compression Codec: " + codec;
    }
}
