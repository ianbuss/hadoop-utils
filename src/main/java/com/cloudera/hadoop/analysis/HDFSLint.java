package com.cloudera.hadoop.analysis;

import com.cloudera.hadoop.detectors.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

public class HDFSLint extends Configured implements Tool {

    private static final int HEADER_READ_LEN = 20;
    private static Detector[] REGISTERED_DETECTORS = {
            new ParquetDetector(),
            new AvroDetector(),
            new SequenceFileDetector(),
            new RCFileDetector(),
            new ORCFileDetector(),
            // put this one last
            new LinuxFileDetector()
    };

    private byte[] header = new byte[HEADER_READ_LEN];
    private Configuration configuration;

    private boolean searchRecursively = false;
    private boolean verbose = true;
    private PrintStream out = System.out;

    public HDFSLint() throws IOException {
        this(new Configuration());
    }

    public HDFSLint(Configuration configuration) throws IOException {
        this.configuration = configuration;
    }

    public HDFSLint searchRecursively(boolean searchRecursively) {
        this.searchRecursively = searchRecursively;
        return this;
    }

    public HDFSLint verbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public HDFSLint out(PrintStream out) {
        this.out = out;
        return this;
    }

    public void inspect(String path) throws IOException {
        PathData pd = new PathData(path, configuration);
        inspect(pd);
    }

    public void inspect(PathData file) throws IOException {
        Summary summary = new Summary(file.path.toString());
        if (file.stat.isDirectory()) {
            for (PathData pd : file.getDirectoryContents()) {
                if (pd.stat.isDirectory() && searchRecursively) {
                    inspect(file);
                } else if (pd.stat.isFile()) {
                    FileReport report = inspectFile(pd);
                    summary.addReport(report);
                    if (verbose) out.println(report.toJson());
                }
            }
        } else {
            FileReport report = inspectFile(file);
            summary.addReport(report);
            if (verbose) out.println(report.toJson());
        }
        System.out.println(summary);
    }

    public FileReport inspectFile(PathData file) throws IOException {
        FSDataInputStream is = file.fs.open(file.path);
        int read = is.read(header, 0, HEADER_READ_LEN);

        try {
            for (Detector detector : REGISTERED_DETECTORS) {
                if (detector.detect(header, read)) {
                    return detector.analyze(file);
                }
            }
            return new FileReport(FileType.OTHER,
              file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length,
              file.stat.getLen(),
              CompressionType.UNKNOWN,
              file.path.toString());
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

    private static String getOptionArg(String[] args, int pos) {
        if (args.length > pos) {
            return args[pos + 1];
        } else {
            exitWithMessage(
              "Could not parse argument for option " + args[pos] + ": " + args[pos + 1], -1);
        }
        return "";
    }

    private static void usageAndExit(int retCode) {
        System.err.printf("Usage: %s [-r] [-q] [-o OUTFILE] [FILE]...\n", HDFSLint.class.getSimpleName());
        System.exit(retCode);
    }

    private static void exitWithMessage(String msg, int retCode) {
        System.err.printf(msg);
        System.exit(retCode);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usageAndExit(-1);
        }

        HDFSLint hdfsLint = new HDFSLint();

        int argInd = 0;
        while (args[argInd].startsWith("-")) {
            if (args[argInd].equals("-r")) {
                hdfsLint.searchRecursively(true);
            } else if (args[argInd].equals("-q")) {
                hdfsLint.verbose(false);
            } else if (args[argInd].equals("-o")) {
                hdfsLint.out(new PrintStream(getOptionArg(args, argInd)));
                argInd++;
            }
            argInd++;
        }

        String[] paths = Arrays.copyOfRange(args, argInd, args.length);

        hdfsLint.run(paths);
    }

}