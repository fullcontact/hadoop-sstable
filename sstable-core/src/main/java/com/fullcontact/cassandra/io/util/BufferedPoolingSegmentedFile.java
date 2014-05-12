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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * Cassandra BufferedPoolingSegmentedFile ported to work with HDFS.
 */
public class BufferedPoolingSegmentedFile extends PoolingSegmentedFile {
    private final FileSystem fs;

    public BufferedPoolingSegmentedFile(String path, long length, FileSystem fs) {
        super(path, length);
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
            long length = new File(path).length();
            return new BufferedPoolingSegmentedFile(path, length, fs);
        }
    }

    protected RandomAccessReader createReader(String path) {
        return RandomAccessReader.open(new Path(path), this, fs);
    }
}
