package com.cloudera.hadoop.detectors;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

public class AvroDetector implements Detector {

    private static final byte[] MAGIC = new byte[] { 'O', 'b', 'j', 0x01 };

    @Override
    public String getName() {
        return "avro";
    }

    @Override
    public boolean detect(byte[] header) {

        if (header.length >= MAGIC.length) {
            for (int i=0; i< MAGIC.length; i++) {
                if (MAGIC[i] != header[i]) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public String analyze(Configuration configuration, FileStatus fileStatus) throws IOException {

        GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
        DataFileReader<Object> fileReader =
                new DataFileReader<Object>(new FsInput(fileStatus.getPath(), configuration), reader);

        String schema = fileReader.getSchema().toString(true);
        String codec = fileReader.getMetaString("avro.codec");

        return "avro file with " + fileReader.getBlockCount() + " blocks \n\nschema: " + schema + "\n\ncompression: " + codec;
    }
}
