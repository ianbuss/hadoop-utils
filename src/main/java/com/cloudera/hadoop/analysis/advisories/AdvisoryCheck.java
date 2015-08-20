package com.cloudera.hadoop.analysis.advisories;

import com.cloudera.hadoop.analysis.FileReport;
import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;

public interface AdvisoryCheck {

  boolean checkForAdvisory(FileReport fileReport, PathData file) throws IOException;

}
