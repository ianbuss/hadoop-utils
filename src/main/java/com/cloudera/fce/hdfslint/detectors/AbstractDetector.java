package com.cloudera.fce.hdfslint.detectors;

import com.cloudera.fce.hdfslint.analysis.FileReport;
import com.cloudera.fce.hdfslint.analysis.advisories.Advisory;
import org.apache.hadoop.fs.shell.PathData;

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
  public boolean detect(byte[] header, int read) {

    if (header.length >= magic.length && read >= magic.length) {
      for (int i=0; i< magic.length; i++) {
        if (magic[i] != header[i]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }

  }

  @Override
  public abstract FileReport analyze(PathData file, String scanDate) throws IOException;

  @Override
  public List<Advisory> checkAdvisories(FileReport fileReport, PathData file) throws IOException {
    List<Advisory> applicableAdvisories = new ArrayList<>();
    for (Advisory advisory : advisories) {
      if (advisory.check.checkForAdvisory(fileReport, file)) {
        applicableAdvisories.add(advisory);
      }
    }

    return applicableAdvisories;
  }
}
