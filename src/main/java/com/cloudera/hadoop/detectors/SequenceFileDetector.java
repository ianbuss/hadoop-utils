package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.PathData;
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
    public FileReport analyze(PathData file) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(file.fs.getConf(),
          SequenceFile.Reader.file(file.path));
        CompressionType compressionType = CompressionType.fromHadoopCodec(reader.getCompressionCodec());
        int blocks = file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length;

        FileReport report = new FileReport(FileType.SEQUENCE, blocks,
          file.stat.getLen(), compressionType, file.path.toString());
        report.addAdvisories(checkAdvisories(file));

        return report;
    }
}
