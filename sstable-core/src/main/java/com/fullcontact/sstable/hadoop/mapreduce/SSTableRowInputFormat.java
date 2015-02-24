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

import com.fullcontact.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.nio.ByteBuffer;

/**
 * Simple row based {@link org.apache.hadoop.mapreduce.InputFormat} implementation.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class SSTableRowInputFormat extends SSTableInputFormat<ByteBuffer, SSTableIdentityIterator> {

    @Override
    public RecordReader<ByteBuffer, SSTableIdentityIterator> createRecordReader(InputSplit split, TaskAttemptContext taskAttempt) {
        return new SSTableRowRecordReader();
    }
}
