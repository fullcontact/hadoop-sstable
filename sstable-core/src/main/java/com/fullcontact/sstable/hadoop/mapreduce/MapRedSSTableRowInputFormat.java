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

import com.fullcontact.sstable.hadoop.SSTableFunctions;
import com.fullcontact.sstable.hadoop.SSTablePredicates;
import com.fullcontact.sstable.index.SSTableIndexIndex;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MapRedSSTableRowInputFormat<K, V> extends FileInputFormat<ByteBuffer, SSTableIdentityIterator> {
    private static final Logger LOG = LoggerFactory.getLogger(MapRedSSTableRowInputFormat.class);
    public static final int COMPRESSION_RATIO_ASSUMPTION = 40;

    private final Map<Path, SSTableIndexIndex> indexes = Maps.newHashMap();

    @Override
    protected FileStatus[] listStatus(JobConf jobConf) throws IOException {

        final List<FileStatus> files = Lists.newArrayList();

        for (FileStatus file : super.listStatus(jobConf)) {
            files.addAll(handleFile(file, jobConf));
        }

        LOG.debug("Initial file list: {} {}", files.size(), files);

        for (Iterator<FileStatus> iterator = files.iterator(); iterator.hasNext(); ) {
            final FileStatus fileStatus = iterator.next();
            final Path file = fileStatus.getPath();
            final FileSystem fs = file.getFileSystem(jobConf);

            if (!SSTablePredicates.IS_SSTABLE.apply(file.toString())) {
                // Ignore non-sstable date files, always (for now)
                LOG.debug("Removing non-sstable file: {}", file);
                iterator.remove();
            } else {
                // read the index file
                LOG.debug("Reading index file for sstable file: {}", file);

                final Path indexFile = SSTableFunctions.INDEX_FILE.apply(file);

                LOG.debug("Reading index file: {}", indexFile);

                final SSTableIndexIndex index = SSTableIndexIndex.readIndex(fs, indexFile);
                indexes.put(file, index);
            }
        }

        LOG.debug("Final file list: {} {}", files.size(), files);

        return files.toArray(new FileStatus[]{});
    }


    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        final List<InputSplit> result = Lists.newArrayList();
        final FileStatus[] files = listStatus(jobConf);

        LOG.debug("Initial file list: {} {}", files.length, files);

        for (final FileStatus fileStatus : files) {
            final Path dataFile = fileStatus.getPath();
            final FileSystem fileSystem = dataFile.getFileSystem(jobConf);
            final BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

            // Data file, try to split if the .index file was found
            final SSTableIndexIndex index = indexes.get(dataFile);
            if (index == null) {
                throw new IOException("Index not found for " + dataFile);
            }

            for (final SSTableIndexIndex.Chunk chunk : index.getOffsets()) {
                // This isn't likely to work well because we are dealing with the index into uncompressed data...
                final int blockIndex = getBlockIndex(blockLocations, chunk.getStart() / COMPRESSION_RATIO_ASSUMPTION);
                final SSTableSplit split = new SSTableSplit(dataFile, chunk.getStart(), chunk.getEnd(),
                    chunk.getEnd() - chunk.getStart(), blockLocations[blockIndex].getHosts());
                result.add(split);
            }
        }

        LOG.debug("Splits calculated: {} {}", result.size(), result);

        return result.toArray(new InputSplit[]{});
    }

    /**
     * If we have a directory recursively gather the files we care about for this job.
     *
     * @param file Root file/directory.
     * @param jobConf Job configuration.
     * @return All files we care about.
     * @throws IOException
     */
    private Collection<FileStatus> handleFile(final FileStatus file, JobConf jobConf) throws
        IOException {
        final List<FileStatus> results = Lists.newArrayList();

        if(file.isDir()) {
            final Path p = file.getPath();
            LOG.debug("Expanding {}", p);
            final FileSystem fs = p.getFileSystem(jobConf);
            final FileStatus[] children = fs.listStatus(p);
            for (FileStatus child : children) {
                results.addAll(handleFile(child, jobConf));
            }
        } else {
            results.add(file);
        }

        return results;
    }
}
