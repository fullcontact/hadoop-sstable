/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fullcontact.cassandra.io.util;

import com.fullcontact.cassandra.io.compress.CompressedRandomAccessReader;
import com.fullcontact.cassandra.io.compress.CompressionMetadata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Cassandra CompressedPoolingSegmentedFile ported to work with HDFS.
 */
public class CompressedPoolingSegmentedFile extends PoolingSegmentedFile implements ICompressedFile {
    public final CompressionMetadata metadata;
    private final FileSystem fs;

    public CompressedPoolingSegmentedFile(String path, CompressionMetadata metadata, FileSystem fs) {
        super(path, metadata.dataLength, metadata.compressedFileLength);
        this.metadata = metadata;
        this.fs = fs;
    }

    public static class Builder extends SegmentedFile.Builder {
        private final FileSystem fs;

        public Builder(FileSystem fs) {

            this.fs = fs;
        }

        public void addPotentialBoundary(long boundary) {
            // only one segment in a standard-io file
        }

        public SegmentedFile complete(String path) {
            return new CompressedPoolingSegmentedFile(path, CompressionMetadata.create(path, fs), fs);
        }
    }

    protected RandomAccessReader createReader(String path) {
        return CompressedRandomAccessReader.open(new Path(path), metadata, this, fs);
    }

    public CompressionMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        metadata.close();
    }
}
