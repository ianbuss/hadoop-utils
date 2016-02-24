package com.cloudera.fce.hdfslint.detectors;

import com.cloudera.fce.hdfslint.analysis.FileReport;
import com.cloudera.fce.hdfslint.analysis.CompressionType;
import com.cloudera.fce.hdfslint.analysis.FileType;
import com.cloudera.fce.hdfslint.analysis.advisories.Advisory;
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
    public FileReport analyze(PathData file, String scanDate) throws IOException {
        ParquetMetadata meta = null;
        try {
            meta = ParquetFileReader.readFooter(file.fs.getConf(),
              file.stat, ParquetMetadataConverter.NO_FILTER);
        } catch (RuntimeException e) {
            // Some Parquet files seem to just contain 'PAR1', catch and
            // do something more intelligent, probably an 'PARQ_EMPTY' advisory
            // TODO: add logging
        }

        // This is a bit naff but just check for any compression
        // in any column in the first block for now
        CompressionType compressionType = CompressionType.UNKNOWN;
        if (meta != null) {
            compressionType = CompressionType.NONE;
            if (meta.getBlocks().size() > 0) {
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
            }
        }

        FileReport report = new FileReport(FileType.PARQUET, meta.getBlocks().size(),
          file.stat.getLen(), compressionType, file.path.toString(), scanDate);
        report.addAdvisories(checkAdvisories(report, file));

        return report;
    }

    @Override
    public List<Advisory> checkAdvisories(FileReport fileReport, PathData file) throws IOException {
        List<Advisory> allAdvisories = super.checkAdvisories(fileReport, file);
        for (Advisory advisory : advisories) {
            if (advisory.check.checkForAdvisory(fileReport, file)) {
                allAdvisories.add(advisory);
            }
        }

        return allAdvisories;
    }

}
