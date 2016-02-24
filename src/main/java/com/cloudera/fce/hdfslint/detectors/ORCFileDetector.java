package com.cloudera.fce.hdfslint.detectors;

import com.cloudera.fce.hdfslint.analysis.FileReport;
import com.cloudera.fce.hdfslint.analysis.CompressionType;
import com.cloudera.fce.hdfslint.analysis.FileType;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;

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
    public FileReport analyze(PathData file, String scanDate) throws IOException {
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
          file.stat.getLen(), compressionType, file.path.toString(), scanDate);
        report.addAdvisories(checkAdvisories(report, file));

        return report;
    }
}
