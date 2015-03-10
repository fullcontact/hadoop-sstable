/*
 * Copyright 2014 FullContact, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fullcontact.sstable.index;

import com.fullcontact.sstable.hadoop.IndexOffsetScanner;
import com.fullcontact.sstable.hadoop.mapreduce.HadoopSSTableConstants;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * This is basically building an index of chunks into the Cassandra Index.db file--an index index. The final index
 * created here defines the splits that will be used downstream. Split size is configurable and defaults to 1024 MB.
 */
public class SSTableIndexIndex {

    public static final String SSTABLE_INDEX_SUFFIX = ".Index";
    public static final String SSTABLE_INDEX_IN_PROGRESS_SUFFIX = ".Index.inprogress";

    private final List<Chunk> chunks = Lists.newArrayList();

    /**
     * Read an existing index. Reads and returns the index index, which is a list of chunks defined by the Cassandra
     * Index.db file along with the configured split size.
     *
     * @param fileSystem Hadoop file system.
     * @param sstablePath SSTable Index.db.
     * @return Index of chunks.
     * @throws IOException
     */
    public static SSTableIndexIndex readIndex(final FileSystem fileSystem, final Path sstablePath) throws IOException {
        final Closer closer = Closer.create();
        final Path indexPath = sstablePath.suffix(SSTABLE_INDEX_SUFFIX);

        // Detonate if we don't have an index.
        final FSDataInputStream inputStream = closer.register(fileSystem.open(indexPath));

        final SSTableIndexIndex indexIndex = new SSTableIndexIndex();
        try {
            while (inputStream.available() != 0) {
                indexIndex.add(inputStream.readLong(), inputStream.readLong());
            }
        } finally {
            closer.close();
        }

        return indexIndex;
    }

    /**
     * Add a chunk with a start and end offset.
     *
     * @param start Beginning of the chunk.
     * @param end End of the chunk.
     */
    private void add(final long start, final long end) {
        this.chunks.add(new Chunk(start, end));
    }

    /**
     * Create and write an index index based on the input Cassandra Index.db file. Read the Index.db and generate chunks
     * (splits) based on the configured chunk size.
     *
     * @param fileSystem Hadoop file system.
     * @param sstablePath SSTable Index.db.
     * @throws IOException
     */
    public static void writeIndex(final FileSystem fileSystem, final Path sstablePath) throws IOException {

        final Configuration configuration = fileSystem.getConf();

        final long splitSize = configuration.getLong(HadoopSSTableConstants.HADOOP_SSTABLE_SPLIT_MB,
                HadoopSSTableConstants.DEFAULT_SPLIT_MB) * 1024 * 1024;

        final Closer closer = Closer.create();

        final Path outputPath = sstablePath.suffix(SSTABLE_INDEX_SUFFIX);
        final Path inProgressOutputPath = sstablePath.suffix(SSTABLE_INDEX_IN_PROGRESS_SUFFIX);

        boolean success = false;
        try {
            final FSDataOutputStream os = closer.register(fileSystem.create(inProgressOutputPath));

            final TLongArrayList splitOffsets = new TLongArrayList();
            long currentStart = 0;
            long currentEnd = 0;
            long currentDataStart = 0;
            long currentDataEnd = 0;
            final IndexOffsetScanner index = closer.register(new IndexOffsetScanner(sstablePath, fileSystem));

            while (index.hasNext()) {
                // NOTE: This does not give an exact size of this split in bytes but a rough estimate.
                // This should be good enough since it's only used for sorting splits by size in hadoop land.
                while (currentDataEnd - currentDataStart < splitSize && index.hasNext()) {
                    final IndexOffsetScanner.IndexEntry indexEntry = index.next();
                    currentEnd = indexEntry.idxOffset;
                    currentDataEnd = indexEntry.dataOffset;
                    splitOffsets.add(currentEnd);
                }

                // Record the split
                final long[] offsets = splitOffsets.toArray();
                os.writeLong(offsets[0]); // Start
                os.writeLong(offsets[offsets.length - 1]); // End

                // Clear the offsets
                splitOffsets.clear();

                if (index.hasNext()) {
                    final IndexOffsetScanner.IndexEntry indexEntry = index.next();
                    currentStart = indexEntry.idxOffset;
                    currentDataStart = indexEntry.dataOffset;
                    currentEnd = currentStart;
                    currentDataEnd = currentDataStart;
                    splitOffsets.add(currentStart);
                }
            }

            success = true;
        } finally {
            closer.close();

            if (!success) {
                fileSystem.delete(inProgressOutputPath, false);
            } else {
                fileSystem.rename(inProgressOutputPath, outputPath);
            }
        }
    }

    /**
     * Return the chunks (splits) defined by this index.
     *
     * @return Chunks.
     */
    public List<Chunk> getOffsets() {
        return Lists.newArrayList(chunks);
    }

    /**
     * Simple chunk which contains a start and end.
     */
    public class Chunk {
        private final long start;
        private final long end;

        public Chunk(final long start, final long end) {
            this.start = start;
            this.end = end;
        }

        public long getEnd() {
            return end;
        }

        public long getStart() {
            return start;
        }

        @Override
        public String toString() {
            return "Chunk{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }
}