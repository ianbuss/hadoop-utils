package com.cloudera.fce.hdfslint;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SplitsReporterTest {

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
    public void testSequenceFile() {
        String[] args = { "seq", "src/test/resources/testseq.seq" };

        SplitsReporter reporter = new SplitsReporter(configuration);
        try {
            reporter.reportSplits(args);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

}
