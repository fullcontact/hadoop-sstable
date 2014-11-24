package com.fullcontact.sstable.example;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fullcontact.sstable.hadoop.mapreduce.SSTableInputFormat;
import com.fullcontact.sstable.hadoop.mapreduce.SSTableRowInputFormat;

/**
 * A simple example demonstrating a possible MapReduce implementation leveraging the sstable reader.
 * <p/>
 * Read sstable data and output as text.
 */
public class SimpleExample extends Configured implements Tool {
	
    private static final Logger LOG = LoggerFactory.getLogger(SimpleExample.class);

    public static void main(String[] args) throws Exception {
//    	Logger.getRootLogger().setLevel(Level.OFF);
        int res = ToolRunner.run(new Configuration(), new SimpleExample(), args);
        System.exit(res);
    }

    private Job getJobConf(CommandLine cli) throws URISyntaxException, IOException {
        Configuration conf = getConf();
        Job job = new Job(conf);
        return job;
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                "SimpleExample <input_dir> <output_dir>", options);
        System.exit(0);
    }

    private static Options buildOptions() {
        Options options = new Options();
        return options;
    }

    @Override
    public int run(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();
        Options options = buildOptions();

        CommandLineParser cliParser = new BasicParser();
        CommandLine cli = cliParser.parse(options, args);
        if (cli.getArgs().length < 2) {
            printUsage(options);
        }
        Job job = getJobConf(cli);

        job.setJobName("Simple Example");

        job.setJarByClass(SimpleExample.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SimpleExampleMapper.class);
        job.setReducerClass(SimpleExampleReducer.class);

        job.setInputFormatClass(SSTableRowInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        String inputPaths = cli.getArgs()[0];
        LOG.info("Setting initial input paths to {}", inputPaths);
        SSTableInputFormat.addInputPaths(job, inputPaths);

        final String outputPath = cli.getArgs()[1];
        LOG.info("Setting initial output paths to {}", outputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        LOG.info("Total runtime: {}s", (System.currentTimeMillis() - startTime) / 1000);
        return success ? 0 : 1;
    }
}