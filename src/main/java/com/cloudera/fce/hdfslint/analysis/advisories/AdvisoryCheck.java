package com.cloudera.fce.hdfslint.analysis.advisories;

import com.cloudera.fce.hdfslint.analysis.FileReport;
import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;

public interface AdvisoryCheck {

  boolean checkForAdvisory(FileReport fileReport, PathData file) throws IOException;

}
