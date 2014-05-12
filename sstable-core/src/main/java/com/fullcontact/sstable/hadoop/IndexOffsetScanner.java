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
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;

/**
 * Index scanner reads offsets from an SSTable index.
 */
public class IndexOffsetScanner implements Closeable {

    private final DataInput input;
    private final Closer closer;

    /**
     * Hadoop fs based version.
     *
     * @param filename File name.
     * @param fileSystem File system.
     */
    public IndexOffsetScanner(final String filename, final FileSystem fileSystem) {
        closer = Closer.create();
        try {
            final FSDataInputStream inputStream = fileSystem.open(new Path(filename));
            this.input = closer.register(new DataInputStream(new FastBufferedInputStream(inputStream)));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Hadoop fs based version.
     *
     * @param path File path.
     * @param fileSystem File system.
     */
    public IndexOffsetScanner(final Path path, final FileSystem fileSystem) {
        closer = Closer.create();
        try {
            final FSDataInputStream inputStream = fileSystem.open(path);
            this.input = closer.register(new DataInputStream(new FastBufferedInputStream(inputStream)));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Java I/O based version.
     *
     * @param filename File name.
     */
    public IndexOffsetScanner(final String filename) {
        closer = Closer.create();
        try {
            this.input = closer.register(new DataInputStream(new BufferedInputStream(new FileInputStream(filename), 65536 * 10)));
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
            return ((DataInputStream) input).available() != 0;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Get the next offset from the SSTable index.
     * @return SSTable offset.
     */
    public long next() {
        try {
            ByteBufferUtil.readWithShortLength(input);

            final long offset = input.readLong();

            // TODO: Because this is version ic > ia promotedIndex is true and we need to handle it. See C* Descriptor
            skipPromotedIndex(input);

            return offset;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Convenience method for skipping promoted index from SSTable index file.
     * @param in DataInput.
     * @throws IOException
     */
    private void skipPromotedIndex(final DataInput in) throws IOException {
        final int size = in.readInt();
        if (size <= 0) {
            return;
        }

        FileUtils.skipBytesFully(in, size);
    }
}
