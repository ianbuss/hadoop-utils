package com.cloudera.hadoop.analysis.advisories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class ParquetFileSplitCheck implements AdvisoryCheck {

  @Override
  public boolean checkForAdvisory(Configuration conf, FileStatus fileStatus) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    int numBlocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;
    return numBlocks > 1;
  }
}
