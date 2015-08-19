package com.cloudera.hadoop.analysis;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HDFSLintTest {

    private Configuration configuration;

    @Before
    public void setup() {
        configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("hadoop.security.authentication", "baboon");
    }

    @Test
    public void testTextFile() throws IOException {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testtext.txt");
    }

    @Test
    public void testParquetFile() throws IOException {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testparq.parq");
    }

    @Test
    public void testAvroFile() throws IOException  {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testavro.avro");
    }

    @Test
    public void testSeqFile() throws IOException {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testseq.seq");
    }

    @Test
    public void testRCFile() throws IOException {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testrcfile.rcf");
    }

    @Test
    public void testORCFile() throws IOException {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testorc.orc");
    }

    @Test
    public void testDir() throws IOException {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources");
    }

}
