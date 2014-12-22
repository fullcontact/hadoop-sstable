package com.fullcontact.sstable.index;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class SSTableSplitInputFormat extends FileInputFormat<Path, LongWritablePair> {

    @Override
    public RecordReader<Path, LongWritablePair> createRecordReader(InputSplit inputSplit,
                                                               TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new SSTableSplitRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // Force the files to be unsplittable, because indexing requires seeing all the
        // compressed blocks in succession.
        return false;
    }
}
