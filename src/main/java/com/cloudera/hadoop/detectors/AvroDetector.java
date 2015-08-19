package com.cloudera.hadoop.detectors;

import com.cloudera.hadoop.analysis.CompressionType;
import com.cloudera.hadoop.analysis.FileReport;
import com.cloudera.hadoop.analysis.FileType;
import com.cloudera.hadoop.analysis.advisories.Advisory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;
import java.util.List;

public class AvroDetector extends AbstractDetector {

    public AvroDetector() {
        magic = new byte[] { 'O', 'b', 'j', 0x01 };
    }

    @Override
    public String getName() {
        return "avro";
    }

    @Override
    public FileReport analyze(PathData file) throws IOException {
        GenericDatumReader<Object> reader = new GenericDatumReader<>();
        DataFileReader<Object> fileReader =
                new DataFileReader<>(new FsInput(file.path, file.fs), reader);

        String codec = fileReader.getMetaString("avro.codec");
        CompressionType compressionType = CompressionType.NONE;

        if (codec.equals("null")) {
            compressionType = CompressionType.NONE;
        } else if (codec.equals("deflate")) {
            compressionType = CompressionType.DEFLATE;
        } else if (codec.equals("snappy")) {
            compressionType = CompressionType.SNAPPY;
        }

        FileReport fileReport = new FileReport(FileType.AVRO, fileReader.getBlockCount(),
          file.stat.getLen(), compressionType, file.path.toString());
        fileReport.addAdvisories(checkAdvisories(file));

        return fileReport;
    }

    @Override
    public List<Advisory> checkAdvisories(PathData file) throws IOException {
        return super.checkAdvisories(file);
    }
}
