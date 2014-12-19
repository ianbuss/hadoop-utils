package com.cloudera.hadoop;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import parquet.hadoop.ParquetInputFormat;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

public class SplitsReporter {

    private static enum InputFormatClass {
        TEXT (TextInputFormat.class),
        SEQ (SequenceFileInputFormat.class),
        AVRO (AvroInputFormat.class),
        PARQUET (ParquetInputFormat.class);

        private final Class clazz;
        InputFormatClass(Class clazz) {
            this.clazz = clazz;
        }

        Class getInputFormat() {
            return clazz;
        }
    }

    private Configuration conf;

    public SplitsReporter() {
        conf = new Configuration();
    }

    public SplitsReporter(Configuration conf) {
        this.conf = conf;
    }

    public void reportSplits(String[] args) throws Exception {
        InputFormatClass inputFormatClass = InputFormatClass.valueOf(args[0].toUpperCase());
        String[] files = Arrays.copyOfRange(args, 1, args.length);

        for (String file : files) {
            Job job = Job.getInstance(conf);
            job.setInputFormatClass(inputFormatClass.getInputFormat());
            FileInputFormat.addInputPath(job, new Path(file));

            Constructor<InputFormat> constructor = inputFormatClass.getInputFormat().getConstructor();
            InputFormat inputFormat = constructor.newInstance();
            List<InputSplit> splits = inputFormat.getSplits(job);

            System.out.println("File: " + file);
            System.out.println("# input splits: " + splits.size());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("Usage: %s (text|seq|avro|parquet) [file [file ...]]\n", SplitsReporter.class.getSimpleName());
        }

        SplitsReporter reporter = new SplitsReporter();
        reporter.reportSplits(args);
    }
}
