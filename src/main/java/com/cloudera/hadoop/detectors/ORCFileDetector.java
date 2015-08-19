package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.PathData;
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
    public FileReport analyze(PathData file) throws IOException {
        Reader reader = OrcFile.createReader(file.fs, file.path);
        CompressionKind compressionKind = reader.getCompression();
        CompressionType compressionType = CompressionType.NONE;
        if (compressionKind.equals(CompressionKind.LZO)) {
            compressionType = CompressionType.LZO;
        } else if (compressionKind.equals(CompressionKind.SNAPPY)) {
            compressionType = CompressionType.SNAPPY;
        } else if (compressionKind.equals(CompressionKind.ZLIB)) {
            compressionType = CompressionType.ZLIB;
        }
        int blocks = file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length;

        FileReport report = new FileReport(FileType.ORC, blocks,
          file.stat.getLen(), compressionType, file.path.toString());
        report.addAdvisories(checkAdvisories(file));

        return report;
    }
}
