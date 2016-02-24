package com.cloudera.fce.hdfslint.detectors;

import com.cloudera.fce.hdfslint.analysis.FileReport;
import com.cloudera.fce.hdfslint.analysis.CompressionType;
import com.cloudera.fce.hdfslint.analysis.FileType;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hive.ql.io.RCFile;

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
    public FileReport analyze(PathData file, String scanDate) throws IOException {

        RCFile.Reader reader = null;
        try {
            reader = new RCFile.Reader(file.fs, file.path, file.fs.getConf());
            int blocks = file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length;
            CompressionType compressionType = CompressionType.fromHadoopCodec(reader.getCompressionCodec());

            FileReport report = new FileReport(FileType.RCFILE, blocks,
              file.stat.getLen(), compressionType, file.path.toString(), scanDate);
            report.addAdvisories(checkAdvisories(report, file));

            return report;
        } finally {
            if (reader != null) reader.close();
        }
    }
}
