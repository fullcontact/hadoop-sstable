package com.fullcontact.sstable.index;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SSTableIndexOutputFormat extends OutputFormat<Path, LongWritablePair> {
    @Override
    public RecordWriter<Path, LongWritablePair> getRecordWriter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new SSTableIndexRecordWriter(taskAttemptContext);
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
    }

    // A totally no-op output committer, because the SstableIndexRecordWriter opens a file on the side
    // and writes to that instead.
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new OutputCommitter() {
            @Override
            public void setupJob(JobContext jobContext) throws IOException {
            }

            @Override
            public void cleanupJob(JobContext jobContext) throws IOException {
            }

            @Override
            public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
            }

            @Override
            public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
            }

            @Override
            public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
                return false;
            }
        };
    }
}
