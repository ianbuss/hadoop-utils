package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import com.cloudera.hadoop.analysis.advisories.Advisory;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.*;
import java.util.List;

public class LinuxFileDetector extends AbstractDetector {

  private Advisory[] advisories = new Advisory[] {
    Advisory.UNCOMPRESSED_TEXT
  };

  @Override
  public boolean detect(byte[] header, int read) {
    return true;
  }

  @Override
  public String getName() {
    return "linux";
  }

  @Override
  public FileReport analyze(PathData file, String scanDate) throws IOException {
    // The file might be compressed. Is it?
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(file.fs.getConf());
    CompressionCodec codec = codecFactory.getCodec(file.path);
    CompressionType compressionType = CompressionType.fromHadoopCodec(codec);

    int blocks = file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length;

    // Default to OTHER
    FileType type = FileType.OTHER;

    // Pipe the first 2KB of the file to the Unix file utility
    String fileDetectionString = "";
    InputStream is = file.fs.open(file.path);
    try {
      if (codec != null) {
        is = codec.createInputStream(is);
      }
      byte[] buf = new byte[2048];
      int read = is.read(buf, 0, 2048);

      if (read > 0) {
        // Replace common Hive delimiters (plus nul byte) - again, this is a bit naff
        for (int i = 0; i < read; i++) {
          if (buf[i] == 0x00 || buf[i] == 0x01 || buf[i] == 0x02 || buf[i] == 0x03) {
            buf[i] = '.';
          }
        }

        ProcessBuilder pb = new ProcessBuilder().command("/usr/bin/file", "-");
        Process process = pb.start();
        try (OutputStream os = process.getOutputStream()) {
          os.write(buf, 0, read);
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String out;
          while ((out = br.readLine()) != null) {
            fileDetectionString += out;
          }
        }

        try {
          process.waitFor();
        } catch (InterruptedException e) {
          process.destroy();
        }

        // Naff naff naff
        if (fileDetectionString.contains("text")) {
          type = FileType.TEXT;
        }
      }
    } finally {
      if (is != null) is.close();
    }

    FileReport report = new FileReport(type, blocks, file.stat.getLen(), compressionType,
      file.path.toString(), scanDate, fileDetectionString);
    report.addAdvisories(checkAdvisories(report, file));

    return report;
  }

  @Override
  public List<Advisory> checkAdvisories(FileReport fileReport, PathData file) throws IOException {
    List<Advisory> allAdvisories = super.checkAdvisories(fileReport, file);
    for (Advisory advisory : advisories) {
      if (advisory.check.checkForAdvisory(fileReport, file)) {
        allAdvisories.add(advisory);
      }
    }

    return allAdvisories;
  }
}
