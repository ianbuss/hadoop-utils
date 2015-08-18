package com.cloudera.hadoop.analysis.advisories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class FileSizeCheck implements AdvisoryCheck {

  private static final float SIZE_RATIO = 0.25f;

  @Override
  public boolean checkForAdvisory(Configuration conf, FileStatus fileStatus) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    float defaultBlockSize = (float) fs.getDefaultBlockSize(fileStatus.getPath());
    float fileSize = (float) fileStatus.getLen();

    if (fileSize / defaultBlockSize < SIZE_RATIO) {
      return true;
    }

    return false;
  }
}
