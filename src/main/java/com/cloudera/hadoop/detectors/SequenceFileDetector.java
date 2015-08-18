package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class SequenceFileDetector extends AbstractDetector {

    private int version;

    public SequenceFileDetector() {
        magic = new byte[] { 'S', 'E', 'Q' };
    }

    @Override
    public String getName() {
        return "SequenceFile";
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
    public FileReport analyze(Configuration configuration, FileStatus fileStatus) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, fileStatus.getPath(), configuration);
        CompressionType compressionType = CompressionType.fromHadoopCodec(reader.getCompressionCodec());
        int blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;

        FileReport report = new FileReport(FileType.SEQUENCE, blocks,
          fileStatus.getLen(), compressionType, fileStatus.getPath().getName());
        report.addAdvisories(checkAdvisories(configuration, fileStatus));

        return report;
    }
}
