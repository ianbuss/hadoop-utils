package com.cloudera.hadoop.detectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

public interface Detector {

    public String getName();

    public boolean detect(byte[] header);

    public String analyze(Configuration configuration, FileStatus fileStatus) throws IOException;

}
