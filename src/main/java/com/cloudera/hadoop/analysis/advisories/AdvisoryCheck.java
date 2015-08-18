package com.cloudera.hadoop.analysis.advisories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

public interface AdvisoryCheck {

  boolean checkForAdvisory(Configuration conf, FileStatus fileStatus) throws IOException;

}
