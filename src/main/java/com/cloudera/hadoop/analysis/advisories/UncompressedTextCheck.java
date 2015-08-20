package com.cloudera.hadoop.analysis.advisories;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;

public class UncompressedTextCheck implements AdvisoryCheck {

  private static final float UNCOMPRESSED_TO_BS_RATIO = 0.5f;

  @Override
  public boolean checkForAdvisory(FileReport fileReport, PathData file) throws IOException {
    float ratio = (float)fileReport.fileSize / (float)file.fs.getDefaultBlockSize(file.path);
    if (fileReport.type.equals(FileType.TEXT) &&
      fileReport.compressionType.equals(CompressionType.NONE) &&
      ratio > UNCOMPRESSED_TO_BS_RATIO)
      return true;
    return false;
  }
}
