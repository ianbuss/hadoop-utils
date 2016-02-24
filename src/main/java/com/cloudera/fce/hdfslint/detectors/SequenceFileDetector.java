package com.cloudera.fce.hdfslint.detectors;

import com.cloudera.fce.hdfslint.analysis.FileReport;
import com.cloudera.fce.hdfslint.analysis.FileType;
import com.cloudera.fce.hdfslint.analysis.CompressionType;
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
    public FileReport analyze(PathData file, String scanDate) throws IOException {
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(file.fs.getConf(),
              SequenceFile.Reader.file(file.path));
            CompressionType compressionType = CompressionType.fromHadoopCodec(reader.getCompressionCodec());
            int blocks = file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length;

            FileReport report = new FileReport(FileType.SEQUENCE, blocks,
              file.stat.getLen(), compressionType, file.path.toString(), scanDate);
            report.addAdvisories(checkAdvisories(report, file));

            return report;
        } finally {
            if (reader != null) reader.close();
        }
    }
}
