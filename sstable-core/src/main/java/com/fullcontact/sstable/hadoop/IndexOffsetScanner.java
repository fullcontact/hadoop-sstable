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
package com.fullcontact.sstable.hadoop;

import com.fullcontact.cassandra.io.util.FileUtils;
import com.google.common.io.Closer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Index scanner reads offsets from an SSTable index.
 */
public class IndexOffsetScanner implements Closeable {

    private final FSDataInputStream input;
    private final Closer closer;


    /**
     * Hadoop fs based version.
     *
     * @param path File path.
     * @param fileSystem File system.
     */
    public IndexOffsetScanner(final Path path, final FileSystem fileSystem) {
        closer = Closer.create();
        try {
            // TODO: this used to wrap w/ some buffering — see if that's still needed
            final FSDataInputStream inputStream = fileSystem.open(path);
            this.input = closer.register(inputStream);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Close any and all resources.
     */
    public void close() {
        try {
            closer.close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * Determine if we have a next record.
     * @return Flag indicating whether we have another record.
     */
    public boolean hasNext() {
        try {
            return input.available() != 0;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Get the next offset from the SSTable index.
     * @return SSTable offset.
     */
    public IndexEntry next() {
        try {
            return IndexEntry.read(input);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Convenience method for skipping promoted index from SSTable index file.
     * @param in DataInput.
     * @throws IOException
     */
    public static void skipPromotedIndex(final DataInput in) throws IOException {
        final int size = in.readInt();
        if (size <= 0) {
            return;
        }

        FileUtils.skipBytesFully(in, size);
    }

    public static class IndexEntry {
        public long idxOffset, dataOffset;
        public ByteBuffer key; // maybe add this

        public static IndexEntry read(FSDataInputStream in) throws IOException {
            IndexEntry retval = new IndexEntry();
            retval.idxOffset = in.getPos();
            ByteBufferUtil.readWithShortLength(in); // ignore key
            retval.dataOffset = in.readLong();
            skipPromotedIndex(in);
            return retval;
        }
    }
}
