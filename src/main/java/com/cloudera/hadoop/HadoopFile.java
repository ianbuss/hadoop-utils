package com.cloudera.hadoop;

import com.cloudera.hadoop.detectors.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class HadoopFile extends Configured implements Tool {

    private static final int HEADER_READ_LEN = 4;
    private static Detector[] REGISTERED_DETECTORS = {
            new ParquetDetector(),
            new AvroDetector(),
            new SequenceFileDetector(),
            new RCFileDetector(),
            new ORCFileDetector()
    };

    String file;
    byte[] header = new byte[HEADER_READ_LEN];
    Configuration configuration;

    public HadoopFile(String file) {
        this.file = file;
        this.configuration = new Configuration();
    }

    public HadoopFile(String file, Configuration configuration) {
        this.file = file;
        this.configuration = configuration;
    }

    public String inspect() throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path filePath = new Path(file);
        FSDataInputStream is = fileSystem.open(filePath);
        int read = is.read(header, 0, HEADER_READ_LEN);

        try {
            for (Detector detector : REGISTERED_DETECTORS) {
                if (detector.detect(header, read)) {
                    PathData pd = new PathData(file, configuration);
                    return(file + ": " + detector.analyze(pd, ""));
                }
            }

            return (file + ": octet-stream");
        }
        finally {
            is.close();
        }

    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println(inspect());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.printf("Usage: %s <file>\n", HadoopFile.class.getSimpleName());
            System.exit(-1);
        }

        HadoopFile hadoopFile = new HadoopFile(args[0]);
        hadoopFile.run(args);
    }

}
