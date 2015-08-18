package com.cloudera.hadoop.analysis;

import com.cloudera.hadoop.detectors.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Arrays;

public class HDFSLint extends Configured implements Tool {

    private static final int HEADER_READ_LEN = 20;
    private static Detector[] REGISTERED_DETECTORS = {
            new ParquetDetector(),
            new AvroDetector(),
            new SequenceFileDetector(),
            new RCFileDetector(),
            new ORCFileDetector()
    };

    private byte[] header = new byte[HEADER_READ_LEN];
    private Configuration configuration;
    private FileSystem fileSystem;

    private boolean searchRecursively = false;

    public HDFSLint() throws IOException {
        this(new Configuration());
    }

    public HDFSLint(Configuration configuration) throws IOException {
        this.configuration = configuration;
        fileSystem = FileSystem.get(configuration);
    }

    public HDFSLint searchRecursively(boolean searchRecursively) {
        this.searchRecursively = searchRecursively;
        return this;
    }

    public void inspect(String path) throws IOException {
        Path fsPath = new Path(path);
        FileStatus fileStatus = fileSystem.getFileStatus(fsPath);
        inspect(fileStatus);
    }

    public void inspect(FileStatus fileStatus) throws IOException {
        if (fileStatus.isDirectory()) {
            for (FileStatus status : fileSystem.listStatus(fileStatus.getPath())) {
                if (status.isDirectory() && searchRecursively) {
                    inspect(status);
                } else if (status.isFile()) {
                    FileReport report = inspectFile(status);
                    System.out.println(report.toJson());
                }
            }
        } else {
            FileReport report = inspectFile(fileStatus);
            System.out.println(report.toJson());
        }
    }

    public FileReport inspectFile(FileStatus fileStatus) throws IOException {
        FSDataInputStream is = fileSystem.open(fileStatus.getPath());
        int read = is.read(header, 0, HEADER_READ_LEN);

        try {
            for (Detector detector : REGISTERED_DETECTORS) {
                if (detector.detect(header, read)) {
                    return detector.analyze(configuration, fileStatus);
                }
            }
            return new FileReport(FileType.OTHER,
              fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()).length,
              fileStatus.getLen(),
              CompressionType.UNKNOWN,
              fileStatus.getPath().getName());
        }
        finally {
            is.close();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        for (String file : strings) {
            inspect(file);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.printf("Usage: %s <file>\n", HDFSLint.class.getSimpleName());
            System.exit(-1);
        }

        HDFSLint hadoopFile = new HDFSLint();

        int argInd = 0;
        if (args[argInd].equals("-r")) {
            hadoopFile.searchRecursively(true);
            argInd++;
        }

        String[] paths = Arrays.copyOfRange(args, argInd, args.length);

        hadoopFile.run(paths);
    }

}