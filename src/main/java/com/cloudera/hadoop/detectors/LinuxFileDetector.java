package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.*;

public class LinuxFileDetector extends AbstractDetector {

  @Override
  public boolean detect(byte[] header, int read) {
    return true;
  }

  @Override
  public String getName() {
    return "linux";
  }

  @Override
  public FileReport analyze(PathData file) throws IOException {
    // The file might be compressed. Is it?
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(file.fs.getConf());
    CompressionCodec codec = codecFactory.getCodec(file.path);
    CompressionType compressionType = CompressionType.fromHadoopCodec(codec);

    int blocks = file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length;

    // Pipe the first 2KB of the file to the Unix file utility
    InputStream is = file.fs.open(file.path);
    if (codec != null) {
      is = codec.createInputStream(is);
    }
    byte[] buf = new byte[2048];
    int read = is.read(buf, 0, 2048);

    ProcessBuilder pb = new ProcessBuilder().command("/usr/bin/file", "-");
    Process process = pb.start();
    OutputStream os = process.getOutputStream();
    os.write(buf, 0, read);
    os.close();
    BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String result = "";
    String out = "";
    while ((out = br.readLine()) != null) {
      result += out;
    }

    try {
      process.waitFor();
    } catch (InterruptedException e) {
      process.destroy();
    }

    // Naff naff naff
    FileType type = FileType.OTHER;
    if (result.contains("text")) {
      type = FileType.TEXT;
    }

    FileReport report = new FileReport(type, blocks, file.stat.getLen(), compressionType,
      file.path.toString());
    report.addAdvisories(checkAdvisories(file));

    return report;
  }
}
