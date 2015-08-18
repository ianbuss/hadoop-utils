package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.advisories.Advisory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.List;

public interface Detector {

    String getName();

    boolean detect(byte[] header, int read);

    FileReport analyze(Configuration configuration, FileStatus fileStatus) throws IOException;

    List<Advisory> checkAdvisories(Configuration configuration, FileStatus fileStatus) throws IOException;

}
