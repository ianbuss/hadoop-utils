package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
    public boolean detect(byte[] header) {

        if (!super.detect(header)) {
            return false;
        }

        if (header.length >= 4) {
            version = (int) header[3];
        }

        return true;
    }

    @Override
    public FileReport analyze(Configuration configuration, FileStatus fileStatus) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        RCFile.Reader reader = new RCFile.Reader(fs, fileStatus.getPath(), configuration);
        int blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;
        CompressionType compressionType = CompressionType.fromHadoopCodec(reader.getCompressionCodec());

        FileReport report = new FileReport(FileType.RCFILE, blocks,
          fileStatus.getLen(), compressionType, fileStatus.getPath().getName());
        report.addAdvisories(checkAdvisories(configuration, fileStatus));

        return report;
    }
}
