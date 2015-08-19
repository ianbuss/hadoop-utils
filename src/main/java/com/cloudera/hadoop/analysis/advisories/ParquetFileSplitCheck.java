package com.cloudera.hadoop.analysis.advisories;

import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;

public class ParquetFileSplitCheck implements AdvisoryCheck {

  @Override
  public boolean checkForAdvisory(PathData file) throws IOException {
    int numBlocks = file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length;
    return numBlocks > 1;
  }
}
