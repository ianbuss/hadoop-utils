package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import com.cloudera.hadoop.analysis.advisories.Advisory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import parquet.format.converter.ParquetMetadataConverter;
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
    public FileReport analyze(PathData file) throws IOException {
        ParquetMetadata meta = ParquetFileReader.readFooter(file.fs.getConf(),
          file.stat, ParquetMetadataConverter.NO_FILTER);

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
          file.stat.getLen(), compressionType, file.path.toString());
        report.addAdvisories(checkAdvisories(file));

        return report;
    }

    @Override
    public List<Advisory> checkAdvisories(PathData file) throws IOException {
        List<Advisory> allAdvisories = super.checkAdvisories(file);
        for (Advisory advisory : advisories) {
            if (advisory.check.checkForAdvisory(file)) {
                allAdvisories.add(advisory);
            }
        }

        return allAdvisories;
    }

}
