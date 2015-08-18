package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.Metadata;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class ORCFileDetector extends AbstractDetector {

    public ORCFileDetector() {
        magic = new byte[] { 'O', 'R', 'C' };
    }

    @Override
    public String getName() {
        return "ORCFile";
    }

    @Override
    public FileReport analyze(Configuration configuration, FileStatus fileStatus) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        Reader reader = OrcFile.createReader(fs, fileStatus.getPath());
        CompressionKind compressionKind = reader.getCompression();
        CompressionType compressionType = CompressionType.NONE;
        if (compressionKind.equals(CompressionKind.LZO)) {
            compressionType = CompressionType.LZO;
        } else if (compressionKind.equals(CompressionKind.SNAPPY)) {
            compressionType = CompressionType.SNAPPY;
        } else if (compressionKind.equals(CompressionKind.ZLIB)) {
            compressionType = CompressionType.ZLIB;
        }
        int blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;

        FileReport report = new FileReport(FileType.ORC, blocks,
          fileStatus.getLen(), compressionType, fileStatus.getPath().getName());
        report.addAdvisories(checkAdvisories(configuration, fileStatus));

        return report;
    }
}
