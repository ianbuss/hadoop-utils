package com.cloudera.hadoop.analysis.advisories;

import com.cloudera.hadoop.analysis.FileReport;
import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;

public class FileSizeCheck implements AdvisoryCheck {

  private static final float SIZE_RATIO = 0.25f;

  @Override
  public boolean checkForAdvisory(FileReport fileReport, PathData file) throws IOException {
    float defaultBlockSize = (float) file.fs.getDefaultBlockSize(file.path);
    float fileSize = (float) file.stat.getLen();

    if (fileSize / defaultBlockSize < SIZE_RATIO) {
      return true;
    }

    return false;
  }
}
