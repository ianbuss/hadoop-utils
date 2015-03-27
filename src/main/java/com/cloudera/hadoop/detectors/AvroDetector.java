package com.cloudera.hadoop.detectors;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class AvroDetector extends AbstractDetector {

    public AvroDetector() {
        magic = new byte[] { 'O', 'b', 'j', 0x01 };
    }

    @Override
    public String getName() {
        return "avro";
    }

    @Override
    public String analyze(Configuration configuration, FileStatus fileStatus) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        int blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length;
        GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
        DataFileReader<Object> fileReader =
                new DataFileReader<Object>(new FsInput(fileStatus.getPath(), configuration), reader);

        String schema = fileReader.getSchema().toString(true);
        String codec = fileReader.getMetaString("avro.codec");

        return "Avro file with " + blocks +
                (blocks == 1 ? " block " : " blocks") + "\n\n" +
                "schema: " + schema + "\n\n" +
                "compression: " + codec;
    }
}
