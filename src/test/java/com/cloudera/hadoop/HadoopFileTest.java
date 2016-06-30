package com.cloudera.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HadoopFileTest {

    private Configuration configuration;

    @Before
    public void setup() {
        configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("hadoop.security.authentication", "baboon");
    }

    @Test
    public void testTextFile() {

    }

    @Test
    public void testParquetFile() throws IOException {
        HadoopFile hadoopFile = new HadoopFile("src/test/resources/testparq.parq", configuration);
        String result = hadoopFile.inspect();
    }

    @Test
    public void testAvroFile() throws IOException  {
        HadoopFile hadoopFile = new HadoopFile("src/test/resources/testavro.avro", configuration);
        String result = hadoopFile.inspect();
    }

    @Test
    public void testSeqFile() throws IOException {
        HadoopFile hadoopFile = new HadoopFile("src/test/resources/testseq.seq", configuration);
        String result = hadoopFile.inspect();
    }

    @Test
    public void testRCFile() throws IOException {
        HadoopFile hadoopFile = new HadoopFile("src/test/resources/testrcfile.rcf", configuration);
        String result = hadoopFile.inspect();
    }

    @Test
    public void testORCFile() throws IOException {
        HadoopFile hadoopFile = new HadoopFile("src/test/resources/testorc.orc", configuration);
        String result = hadoopFile.inspect();
    }

}
