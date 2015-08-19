package com.cloudera.hadoop.analysis.advisories;

import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;

public interface AdvisoryCheck {

  boolean checkForAdvisory(PathData file) throws IOException;

}
