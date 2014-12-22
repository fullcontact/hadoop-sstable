package com.fullcontact.sstable.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DistributedSStableIndexIndexer extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(DistributedSStableIndexIndexer.class);
    private final String INDEX_EXTENSION = "-Index.db";

    private final PathFilter nonTemporaryFilter = new PathFilter() {
        public boolean accept(Path path) {
            return !path.toString().endsWith("/_temporary");
        }
    };

    private void walkPath(Path path, PathFilter pathFilter, List<Path> accumulator) {
        try {
            FileSystem fs = path.getFileSystem(getConf());
            FileStatus fileStatus = fs.getFileStatus(path);

            if (fileStatus.isDir()) {
                FileStatus[] children = fs.listStatus(path, pathFilter);
                for (FileStatus childStatus : children) {
                    walkPath(childStatus.getPath(), pathFilter, accumulator);
                }
            } else if (path.toString().endsWith(INDEX_EXTENSION)) {
                Path sstableIndexPath = path.suffix(SSTableIndexIndex.SSTABLE_INDEX_SUFFIX);
                if (fs.exists(sstableIndexPath)) {
                    // If the index exists and is of nonzero size, we're already done.
                    // We re-index a file with a zero-length index, because every file has at least one block.
                    if (fs.getFileStatus(sstableIndexPath).getLen() > 0) {
                        LOG.info("[SKIP] SSTABLE index file already exists for " + path);
                        return;
                    } else {
                        LOG.info("Adding SSTABLE file " + path + " to indexing list (index file exists but is zero length)");
                        accumulator.add(path);
                    }
                } else {
                    // If no index exists, we need to index the file.
                    LOG.info("Adding SSTABLE file " + path + " to indexing list (no index currently exists)");
                    accumulator.add(path);
                }
            }
        } catch (IOException ioe) {
            LOG.warn("Error walking path: " + path, ioe);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length == 0 || (args.length == 1 && "--help".equals(args[0]))) {
            printUsage();
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        List<Path> inputPaths = new ArrayList<Path>();
        for (String strPath : args) {
            walkPath(new Path(strPath), nonTemporaryFilter, inputPaths);
        }

        if (inputPaths.isEmpty()) {
            System.err.println("No input paths found - perhaps all " +
                    ".sstable files have already been indexed.");
            return 0;
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJobName("Distributed SSTable Indexer " + Arrays.toString(args));

        job.setOutputKeyClass(Path.class);
        job.setOutputValueClass(LongWritablePair.class);

        // The SstableIndexOutputFormat doesn't currently work with speculative execution.
        // Patches welcome.
        job.getConfiguration().setBoolean(
                "mapred.map.tasks.speculative.execution", false);

        job.setJarByClass(DistributedSStableIndexIndexer.class);
        job.setInputFormatClass(SSTableSplitInputFormat.class);
        job.setOutputFormatClass(SSTableIndexOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(Mapper.class);

        for (Path p : inputPaths) {
            FileInputFormat.addInputPath(job, p);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DistributedSStableIndexIndexer(), args);
        System.exit(exitCode);
    }

    public static void printUsage() {
        System.err.println("Usage: hadoop jar /path/to/this/jar com.hadoop.compression.sstable.DistributedSstableIndexer <file.sstable | directory> [file2.sstable directory3 ...]");
    }
}
