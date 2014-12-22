package com.fullcontact.sstable.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SSTableIndexRecordWriter extends RecordWriter<Path, LongWritablePair> {
    private static final Log LOG = LogFactory.getLog(SSTableIndexRecordWriter.class);

    private FSDataOutputStream outputStream;
    private final TaskAttemptContext context;

    private FileSystem fs;
    private Path tmpIndexPath;
    private Path realIndexPath;

    public SSTableIndexRecordWriter(TaskAttemptContext taskAttemptContext) {
        context = taskAttemptContext;
    }

    @Override
    public void write(Path path, LongWritablePair offsets) throws IOException, InterruptedException {
        if (outputStream == null) {
            // Set up the output file on the first record.
            LOG.info("Setting up output stream to write index file for " + path);
            outputStream = setupOutputFile(path);
        }
        offsets.write(outputStream);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (outputStream != null) {
            // Close the output stream so that the tmp file is synced, then move it.
            outputStream.close();

            LOG.info("In close, now renaming " + tmpIndexPath + " to final location " + realIndexPath);
            // Rename, indexing completed.
            fs.rename(tmpIndexPath, realIndexPath);
        }
    }

    private FSDataOutputStream setupOutputFile(Path path) throws IOException {
        fs = path.getFileSystem(context.getConfiguration());

        // For /a/b/c.lzo, tmpIndexPath = /a/b/c.lzo.index.tmp,
        // and it is moved to realIndexPath = /a/b/c.lzo.index upon completion.
        tmpIndexPath = path.suffix(SSTableIndexIndex.SSTABLE_INDEX_IN_PROGRESS_SUFFIX);
        realIndexPath = path.suffix(SSTableIndexIndex.SSTABLE_INDEX_SUFFIX);

        // Delete the old index files if they exist.
        fs.delete(tmpIndexPath, false);
        fs.delete(realIndexPath, false);

        return fs.create(tmpIndexPath, false);
    }
}
