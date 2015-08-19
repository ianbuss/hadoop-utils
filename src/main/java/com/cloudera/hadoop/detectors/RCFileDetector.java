package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.*;

import java.io.IOException;

public class RCFileDetector extends AbstractDetector {

    private int version;

    public RCFileDetector() {
        magic = new byte[] { 'R', 'C', 'F' };
    }

    @Override
    public String getName() {
        return "RCFile";
    }

    @Override
    public boolean detect(byte[] header, int read) {

        if (!super.detect(header, read)) {
            return false;
        }

        if (header.length >= 4) {
            version = (int) header[3];
        }

        return true;
    }

    @Override
    public FileReport analyze(PathData file) throws IOException {

        RCFile.Reader reader = new RCFile.Reader(file.fs, file.path, file.fs.getConf());
        int blocks = file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length;
        CompressionType compressionType = CompressionType.fromHadoopCodec(reader.getCompressionCodec());

        FileReport report = new FileReport(FileType.RCFILE, blocks,
          file.stat.getLen(), compressionType, file.path.toString());
        report.addAdvisories(checkAdvisories(file));

        return report;
    }
}
