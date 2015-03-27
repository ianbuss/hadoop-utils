package com.cloudera.hadoop.detectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

public abstract class AbstractDetector implements Detector {

  protected byte[] magic;

  @Override
  public abstract String getName();

  @Override
  public boolean detect(byte[] header) {

    if (header.length >= magic.length) {
      for (int i=0; i< magic.length; i++) {
        if (magic[i] != header[i]) {
          return false;
        }
      }
    }

    return true;

  }

  @Override
  public abstract String analyze(Configuration configuration,
                                 FileStatus fileStatus) throws IOException;
}
