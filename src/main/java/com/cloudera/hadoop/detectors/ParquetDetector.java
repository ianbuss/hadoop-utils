package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import com.cloudera.hadoop.analysis.advisories.Advisory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.List;

public class ParquetDetector extends AbstractDetector {

    private Advisory[] advisories = new Advisory[] {
      Advisory.PARQ_SPLIT
    };

    public ParquetDetector() {
        magic = new byte[] { 'P', 'A', 'R', '1' };
    }

    @Override
    public String getName() {
        return "parquet";
    }

    @Override
    public FileReport analyze(Configuration configuration, FileStatus fileStatus) throws IOException {
        ParquetMetadata meta = ParquetFileReader.readFooter(configuration, fileStatus);

        // This is a bit naff but just check for any compression
        // in any column in the first block for now
        CompressionType compressionType = CompressionType.NONE;
        for (ColumnChunkMetaData metaData : meta.getBlocks().get(0).getColumns()) {
            CompressionCodecName codecName = metaData.getCodec();
            if (codecName.equals(CompressionCodecName.GZIP)) {
                compressionType = CompressionType.GZIP;
                break;
            } else if (codecName.equals(CompressionCodecName.LZO)) {
                compressionType = CompressionType.LZO;
                break;
            } else if (codecName.equals(CompressionCodecName.SNAPPY)) {
                compressionType = CompressionType.SNAPPY;
                break;
            }
        }

        FileReport report = new FileReport(FileType.PARQUET, meta.getBlocks().size(),
          fileStatus.getLen(), compressionType, fileStatus.getPath().getName());
        report.addAdvisories(checkAdvisories(configuration, fileStatus));

        return report;
    }

    @Override
    public List<Advisory> checkAdvisories(Configuration configuration, FileStatus fileStatus) throws IOException {
        List<Advisory> allAdvisories = super.checkAdvisories(configuration, fileStatus);
        for (Advisory advisory : advisories) {
            if (advisory.check.checkForAdvisory(configuration, fileStatus)) {
                allAdvisories.add(advisory);
            }
        }

        return allAdvisories;
    }

}
