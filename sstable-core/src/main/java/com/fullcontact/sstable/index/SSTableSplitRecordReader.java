package com.fullcontact.sstable.index;

import com.fullcontact.sstable.hadoop.IndexOffsetScanner;
import com.fullcontact.sstable.hadoop.mapreduce.HadoopSSTableConstants;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SSTableSplitRecordReader extends RecordReader<Path, LongWritablePair> {
    private static final Log LOG = LogFactory.getLog(SSTableSplitRecordReader.class);

    private final int LOG_EVERY_N_BLOCKS = 10;

    private IndexOffsetScanner index;

    private final LongWritablePair curValue = new LongWritablePair(-1, -1);
    private FSDataInputStream rawInputStream;
    private TaskAttemptContext context;

    private int numBlocksRead = 0;
    private long totalFileSize = 0;
    private Path sstableFile;

    private long splitSize;

    final TLongArrayList splitOffsets = new TLongArrayList();
    long currentStart = 0;
    long currentEnd = 0;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext taskAttemptContext) throws IOException {

        this.context = taskAttemptContext;
        FileSplit fileSplit = (FileSplit) genericSplit;
        this.sstableFile = fileSplit.getPath();
        // The SSTableSplitInputFormat is not splittable, so the split length is the whole file.
        this.totalFileSize = fileSplit.getLength();


        Configuration conf = context.getConfiguration();
        FileSystem fs = sstableFile.getFileSystem(conf);
        this.rawInputStream = fs.open(sstableFile);

        this.splitSize = conf.getLong(HadoopSSTableConstants.HADOOP_SSTABLE_SPLIT_MB,
                HadoopSSTableConstants.DEFAULT_SPLIT_MB) * 1024 * 1024;

        this.index = new IndexOffsetScanner(sstableFile, fs);
    }

    @Override
    public boolean nextKeyValue() throws IOException {


            if (index.hasNext()) {
                // NOTE: This does not give an exact size of this split in bytes but a rough estimate.
                // This should be good enough since it's only used for sorting splits by size in hadoop land.
                while (currentEnd - currentStart < splitSize && index.hasNext()) {
                    currentEnd = index.next();
                    splitOffsets.add(currentEnd);
                }

                // Record the split
                final long[] offsets = splitOffsets.toArray();
                curValue.set_1(offsets[0]);
                curValue.set_2(offsets[offsets.length - 1]);
                numBlocksRead++;

                // Clear the offsets
                splitOffsets.clear();

                if (index.hasNext()) {
                    currentStart = index.next();
                    currentEnd = currentStart;
                    splitOffsets.add(currentStart);
                }

                // Log some progress every so often.
                if (numBlocksRead % LOG_EVERY_N_BLOCKS == 0) {
                    LOG.info("Reading block " + numBlocksRead + " at pos " + currentStart + " of " + totalFileSize + ". Read is " +
                            (100.0 * getProgress()) + "% done. ");
                }
            } else {
                return false;
            }

        return true;
    }

    @Override
    public Path getCurrentKey() {
        return sstableFile;
    }

    @Override
    public LongWritablePair getCurrentValue() {
        return curValue;
    }

    @Override
    public float getProgress() throws IOException {
        if (totalFileSize == 0) {
            return 0.0f;
        } else {
            return (float) rawInputStream.getPos() / totalFileSize;
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing input stream after reading " + numBlocksRead + " blocks from " + sstableFile);
        rawInputStream.close();
    }
}

