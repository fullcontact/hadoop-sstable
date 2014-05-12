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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * SSTable split definition.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class SSTableSplit extends InputSplit implements Writable {

    private long start;
    private long end;
    private long length;
    private Path file;
    private String[] hosts;

    /**
     * Constructs a split with host information
     *
     * @param file  the file name
     * @param hosts the list of hosts containing the block, possibly null
     */
    public SSTableSplit(Path file, long[] offsets, long length, String[] hosts) {
        this(file, offsets[0], offsets[offsets.length - 1], length, hosts);
    }

    public SSTableSplit(Path file, long start, long end, long length, String[] hosts) {
        this.file = file;
        this.length = length;
        this.start = start;
        this.end = end;
        this.hosts = hosts;
    }

    @Override
    public String toString() {
        return "SSTableSplit{" +
                "start=" + start +
                ", end=" + end +
                ", file=" + file +
                ", length=" + length +
                ", hosts=" + Arrays.toString(hosts) +
                '}';
    }

    public long getOffsetCount() {
        return end - start;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file.toString());
        out.writeLong(length);
        out.writeLong(start);
        out.writeLong(end);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file = new Path(Text.readString(in));
        length = in.readLong();
        start = in.readLong();
        end = in.readLong();
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
        return file;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public int getSize() {
        return (int) (end - start);
    }
}
