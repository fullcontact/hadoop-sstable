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

package com.fullcontact.sstable.hadoop.mapreduce;

import com.fullcontact.sstable.hadoop.IndexOffsetScanner;
import com.fullcontact.sstable.hadoop.SSTableFunctions;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * SSTable split definition.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class SSTableSplit extends InputSplit implements Writable {

    private long dataStart, dataEnd;
    private FSDataInputStream indexReader;

    private long idxStart; // remove?
    private long idxEnd;   // remove?

    private long length;
    private Path dataFile;
    private Path indexFile;
    private String[] hosts;

    public SSTableSplit() {
    }

    /**
     * Constructs a split with host information - FOR TESTING
     *
     * @param dataFile  the dataFile name
     * @param hosts the list of hosts containing the block, possibly null
     */
    public SSTableSplit(Path dataFile, long[] offsets, long length, String[] hosts, FileSystem fs)
        throws IOException
    {
        this(dataFile, offsets[0], offsets[offsets.length - 1], length, hosts, new LocalFileSystem());
    }

    public SSTableSplit(Path dataFile, long start, long end, long length, String[] hosts, FileSystem fs)
        throws  IOException
    {
        this.dataFile = dataFile;
        this.indexFile = SSTableFunctions.INDEX_FILE.apply(dataFile);
        this.indexReader = fs.open(indexFile);
        this.length = length;
        this.idxStart = start;
        this.idxEnd = end;

        indexReader.seek(idxStart);
        ByteBuffer ignoredKey = ByteBufferUtil.readWithShortLength(indexReader);
        this.dataStart = indexReader.readLong();

        indexReader.seek(idxEnd);
        ignoredKey = ByteBufferUtil.readWithShortLength(indexReader);
        this.dataEnd = indexReader.readLong();

        // back to "zero" (for this split)
        indexReader.seek(idxStart);

        this.hosts = hosts;
    }

    @Override
    public String toString() {  // TODO
        return "SSTableSplit{" +
                "idxStart=" + idxStart +
                ", idxEnd=" + idxEnd +
                ", dataFile=" + dataFile +
                ", length=" + length +
                ", hosts=" + Arrays.toString(hosts) +
                '}';
    }

    public long getOffsetCount() {
        return idxEnd - idxStart;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    @Override
    public void write(DataOutput out) throws IOException { // TODO
        Text.writeString(out, dataFile.toString());
        out.writeLong(length);
        out.writeLong(idxStart);
        out.writeLong(idxEnd);
    }

    @Override
    public void readFields(DataInput in) throws IOException { // TODO
        dataFile = new Path(Text.readString(in));
        length = in.readLong();
        idxStart = in.readLong();
        idxEnd = in.readLong();
        hosts = null;
    }

    @Override
    public String[] getLocations() throws IOException {
        if (this.hosts == null) {
            return new String[]{};
        } else {
            return this.hosts;
        }
    }

    public Path getPath() {
        return dataFile;
    }

    /**
     * Given an offset into the index file, return the corresponding index into the data file.
     */
    public long getStart() {
        return dataStart;
    }

    public long getEnd() {
        return dataEnd;
    }

    public int getIndexSize() {
        return (int) (idxEnd - idxStart);
    }

    // warning - STATEFUL
    // TODO move some of this to IndexOffsetScanner
    public long getDataSize() throws IOException {
        ByteBuffer ignoredKey = ByteBufferUtil.readWithShortLength(indexReader);
        long dataStart = indexReader.readLong();
        IndexOffsetScanner.skipPromotedIndex(indexReader);
        long savePos = indexReader.getPos();
        ignoredKey = ByteBufferUtil.readWithShortLength(indexReader);
        long dataEnd = indexReader.readLong();
        IndexOffsetScanner.skipPromotedIndex(indexReader);

        indexReader.seek(savePos);
        return dataEnd - dataStart;
    }

    /**
     * Lazy loads Index.db
     */
    private long[] getIndex() {
        return new long[]{}; // TODO
    }

}
