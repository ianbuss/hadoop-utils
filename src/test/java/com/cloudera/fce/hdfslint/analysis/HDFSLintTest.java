package com.cloudera.fce.hdfslint.analysis;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class HDFSLintTest {

    private Configuration configuration;

    @Before
    public void setup() {
        configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("hadoop.security.authentication", "baboon");
    }

    @Test
    public void testTextFile() throws Exception {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testtext.txt");
    }

    @Test
    public void testParquetFile() throws Exception {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testparq.parq");
    }

    @Test
    public void testAvroFile() throws Exception  {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testavro.avro");
    }

    @Test
    public void testSeqFile() throws Exception {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testseq.seq");
    }

    @Test
    public void testRCFile() throws Exception {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testrcfile.rcf");
    }

    @Test
    public void testORCFile() throws Exception {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources/testorc.orc");
    }

    @Test
    public void testDir() throws Exception {
        HDFSLint hadoopFile = new HDFSLint(configuration);
        hadoopFile.inspect("src/test/resources");
    }

}
