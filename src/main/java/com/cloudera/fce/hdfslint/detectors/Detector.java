package com.cloudera.fce.hdfslint.detectors;

import com.cloudera.fce.hdfslint.analysis.FileReport;
import com.cloudera.fce.hdfslint.analysis.advisories.Advisory;
import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;
import java.util.List;

public interface Detector {

    String getName();

    boolean detect(byte[] header, int read);

    FileReport analyze(PathData file, String scanDate) throws IOException;

    List<Advisory> checkAdvisories(FileReport fileReport, PathData file) throws IOException;

}
