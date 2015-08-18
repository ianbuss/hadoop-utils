package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.advisories.Advisory;
import com.cloudera.hadoop.analysis.FileReport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDetector implements Detector {

  protected byte[] magic;

  private Advisory[] advisories = new Advisory[] {
    Advisory.FILE_SIZE
  };

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
    } else {
      return false;
    }

    return true;

  }

  @Override
  public abstract FileReport analyze(Configuration configuration,
                                 FileStatus fileStatus) throws IOException;

  @Override
  public List<Advisory> checkAdvisories(Configuration configuration,
                                  FileStatus fileStatus) throws IOException {
    List<Advisory> applicableAdvisories = new ArrayList<>();
    for (Advisory advisory : advisories) {
      if (advisory.check.checkForAdvisory(configuration, fileStatus)) {
        applicableAdvisories.add(advisory);
      }
    }

    return applicableAdvisories;
  }
}
