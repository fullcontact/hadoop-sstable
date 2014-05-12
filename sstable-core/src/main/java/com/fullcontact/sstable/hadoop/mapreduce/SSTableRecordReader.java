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

import com.fullcontact.cassandra.io.compress.CompressedRandomAccessReader;
import com.fullcontact.cassandra.io.compress.CompressionMetadata;
import com.fullcontact.cassandra.io.util.RandomAccessReader;
import com.google.common.base.Preconditions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateColumnFamilyStatement;
import org.apache.cassandra.dht.AbstractPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Handle reading individual records from an Cassandra SSTable.
 *
 * Uses an SSTableSplit in combination with a CompressedRandomAccessReader to read a section of each SSTable.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public abstract class SSTableRecordReader<K, V> extends RecordReader<K, V> {

    private SSTableSplit split;
    private CompressedRandomAccessReader reader;

    private K key;
    private V value;

    private CFMetaData cfMetaData;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (SSTableSplit) inputSplit;

        final FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        final CompressionMetadata compressionMetadata =
                CompressionMetadata.create(split.getPath().toString(), fileSystem);
        if (compressionMetadata == null) {
            throw new IOException("Compression metadata for file " + split.getPath() + " not found, cannot run");
        }

        // open the file and seek to the start of the split
        this.reader = CompressedRandomAccessReader.open(split.getPath(), compressionMetadata, false, fileSystem);
        this.reader.seek(split.getStart());

        this.cfMetaData = initializeCfMetaData(context);
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    protected void setCurrentKey(K key) {
        this.key = key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    protected void setCurrentValue(V value) {
        this.value = value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (split.getSize() == 0) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (reader.getFilePointer() - split.getStart()) / (float) (split.getSize()));
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    protected RandomAccessReader getReader() {
        return reader;
    }

    protected Path getDataPath() {
        return split.getPath();
    }

    protected boolean hasMore() {
        return reader.getFilePointer() < split.getEnd();
    }

    private static CFMetaData initializeCfMetaData(TaskAttemptContext context) {
        final String cql = context.getConfiguration().get(HadoopSSTableConstants.HADOOP_SSTABLE_CQL);
        Preconditions.checkNotNull(cql, "Cannot proceed without CQL definition.");

        final CreateColumnFamilyStatement statement = getCreateColumnFamilyStatement(cql);

        final CFMetaData cfMetaData;

        try {
            cfMetaData = statement.getCFMetaData();
        } catch (RequestValidationException e) {
            // Cannot proceed if an error occurs
            throw new RuntimeException("Error configuring SSTable reader. Cannot proceed", e);
        }

        return cfMetaData;
    }

    private static CreateColumnFamilyStatement getCreateColumnFamilyStatement(String cql) {
        CreateColumnFamilyStatement statement;
        try {
            statement = (CreateColumnFamilyStatement) QueryProcessor.parseStatement(cql).prepare().statement;
        } catch (RequestValidationException e) {
            // Cannot proceed if an error occurs
            throw new RuntimeException("Error configuring SSTable reader. Cannot proceed", e);
        }
        return statement;
    }

    protected AbstractPartitioner getPartitioner() {
        return new RandomPartitioner();
    }

    protected CFMetaData getCfMetaData() {
        return this.cfMetaData;
    }
}
