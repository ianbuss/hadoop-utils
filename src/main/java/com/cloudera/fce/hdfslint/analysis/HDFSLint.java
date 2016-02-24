package com.cloudera.fce.hdfslint.analysis;

import com.cloudera.fce.hdfslint.detectors.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.util.Tool;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    private static final int SOLR_COMMIT_BATCH = 100;

    private byte[] header = new byte[HEADER_READ_LEN];
    private Configuration configuration;
    private String scanDate;

    private boolean searchRecursively = false;
    private boolean verbose = true;
    private boolean addToSolr = false;
    private String solrCollection;
    private String zookeeperEnsemble;
    private PrintStream out = System.out;
    private CloudSolrServer solrServer;

    public HDFSLint() throws IOException {
        this(new Configuration());
    }

    public HDFSLint(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.scanDate = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss'Z").print(LocalDateTime.now());
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

    public HDFSLint addToSolr(boolean addToSolr) {
        this.addToSolr = addToSolr;
        return this;
    }

    public HDFSLint solrCollection(String solrCollection) {
        this.solrCollection = solrCollection;
        return this;
    }

    public HDFSLint zookeeperEnsemble(String zookeeperEnsemble) {
        this.zookeeperEnsemble = zookeeperEnsemble;
        return this;
    }

    public void inspect(String path) throws IOException, SolrServerException {
        PathData pd = new PathData(path, configuration);
        inspect(pd);
    }

    public Summary inspect(PathData file) throws IOException, SolrServerException {
        Summary summary = new Summary(file.path.toString());
        if (file.stat.isDirectory()) {
            List<SolrInputDocument> batch = new ArrayList<>();
            for (PathData pd : file.getDirectoryContents()) {
                if (pd.stat.isDirectory() && searchRecursively) {
                    summary.addSummary(inspect(pd));
                } else if (pd.stat.isFile()) {
                    FileReport report = inspectFile(pd, scanDate);
                    summary.addReport(report);
                    if (verbose) out.println(report.toJson());
                    if (addToSolr) {
                        batch.add(report.toSolrDoc());
                        if (batch.size() > SOLR_COMMIT_BATCH) {
                            solrServer.add(batch);
                            solrServer.commit();
                            batch.clear();
                        }
                    }
                }
            }
            if (verbose) System.out.println(summary.toJson());
            if (addToSolr && !batch.isEmpty()) {
                solrServer.add(batch);
                solrServer.commit();
            }
        } else {
            FileReport report = inspectFile(file, scanDate);
            summary.addReport(report);
            if (verbose) out.println(report.toJson());
            if (addToSolr) {
                solrServer.add(report.toSolrDoc());
                solrServer.commit();
            }
        }
        return summary;
    }

    public FileReport inspectFile(PathData file, String scanDate) throws IOException {
        try (FSDataInputStream is = file.fs.open(file.path)) {
            int read = is.read(header, 0, HEADER_READ_LEN);
            for (Detector detector : REGISTERED_DETECTORS) {
                if (detector.detect(header, read)) {
                    return detector.analyze(file, scanDate);
                }
            }
            return new FileReport(FileType.OTHER,
              file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen()).length,
              file.stat.getLen(),
              CompressionType.UNKNOWN,
              file.path.toString(),
              scanDate);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        // Setup Solr
        if (addToSolr) {
            if (solrCollection == null || solrCollection.isEmpty()) exitWithMessage("No Solr collection specified", -1);
            if (zookeeperEnsemble == null || zookeeperEnsemble.isEmpty()) exitWithMessage("No ZooKeeper ensemble specified", -1);

            solrServer = new CloudSolrServer(zookeeperEnsemble);
            solrServer.setDefaultCollection(solrCollection);
        }

        for (String file : strings) {
            inspect(file);
        }
        return 0;
    }

    private static String getOptionArg(String[] args, int pos) {
        if (args.length > pos) {
            return args[pos + 1];
        } else {
            exitWithMessage("Could not parse argument for option " + args[pos] + ": " + args[pos + 1], -1);
        }
        return "";
    }

    private static void usageAndExit(int retCode) {
        System.err.printf("Usage: %s [-r] [-q] [-o OUTFILE] [-s COLLECTION] [-z ZOOKEEPERS] [FILE]...\n", HDFSLint.class.getSimpleName());
        System.exit(retCode);
    }

    private static void exitWithMessage(String msg, int retCode) {
        System.err.println(msg);
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
            } else if (args[argInd].equals("-s")) {
                hdfsLint.addToSolr(true).solrCollection(getOptionArg(args, argInd));
                argInd++;
            } else if (args[argInd].equals("-z")) {
                hdfsLint.zookeeperEnsemble(getOptionArg(args, argInd));
                argInd++;
            }
            argInd++;
        }

        String[] paths = Arrays.copyOfRange(args, argInd, args.length);

        hdfsLint.run(paths);
    }

}