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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public class SSTableIndexIndexer {
    private static final Logger LOG = LoggerFactory.getLogger(SSTableIndexIndexer.class);

    private final Configuration configuration;
    private final DecimalFormat decimalFormat;
    private final ListeningExecutorService service;
    private static final String SST_EXTENSION = "-Index.db";
    private final FileSystem fileSystem;

    public SSTableIndexIndexer(final Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.decimalFormat = new DecimalFormat("#0.00");
        this.service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        this.fileSystem = FileSystem.get(configuration);
    }

    public void index(final Path sstablePath) throws IOException {

        final FileStatus fileStatus = fileSystem.getFileStatus(sstablePath);

        if (fileStatus.isDir()) {
            LOG.info("SSTable Indexing directory {}", sstablePath);
            final FileStatus[] statuses = fileSystem.listStatus(sstablePath);
            for (final FileStatus childStatus : statuses) {
                index(childStatus.getPath());
            }
        } else if (sstablePath.toString().endsWith(SST_EXTENSION)) {
            final Path sstableIndexPath = new Path(sstablePath.toString() + SSTableIndexIndex.SSTABLE_INDEX_SUFFIX);
            if (fileSystem.exists(sstableIndexPath)) {
                LOG.info("Skipping as SSTable index file already exists for {}", sstablePath);
            } else {
                // Kick a thread for the index.
                final ListenableFuture<IndexRequest> indexFuture = service.submit(new Callable<IndexRequest>() {
                    @Override
                    public IndexRequest call() throws Exception {
                        final long startTime = System.currentTimeMillis();
                        final long fileSize = fileStatus.getLen();

                        LOG.info("Indexing SSTABLE Indexing file {}, size {} GB...", sstablePath,
                                decimalFormat.format(fileSize / (1024.0 * 1024.0 * 1024.0)));

                        indexSingleFile(fileSystem, sstablePath);

                        return new IndexRequest(sstableIndexPath, startTime, fileSize);
                    }
                });

                Futures.addCallback(indexFuture, new FutureCallback<IndexRequest>() {
                    public void onSuccess(final IndexRequest indexRequest) {
                        long indexSize = 0;

                        try {
                            indexSize = fileSystem.getFileStatus(indexRequest.getIndexPath()).getLen();
                        } catch (IOException e) {
                            LOG.error("Error getting file status for index path: {}", indexRequest.getIndexPath());
                        }

                        final double elapsed = (System.currentTimeMillis() - indexRequest.getStartTime()) / 1000.0;

                        LOG.info("Completed SSTABLE Indexing in {} seconds ({} MB/s).  Index size is {} KB.",
                                decimalFormat.format(elapsed),
                                decimalFormat.format(indexRequest.getFileSize() / (1024.0 * 1024.0 * elapsed)),
                                decimalFormat.format(indexSize / 1024.0));
                    }

                    public void onFailure(Throwable e) {
                        LOG.error("Failed to index.", e);
                    }
                });

            }
        }
    }

    private void indexSingleFile(final FileSystem fileSystem, final Path sstablePath) {
        try {
            SSTableIndexIndex.writeIndex(fileSystem, sstablePath);
        } catch (IOException e) {
            LOG.error("Error indexing {}", sstablePath);
            LOG.error("Error indexing", e);
        }
    }

    public class IndexRequest {

        private final Path indexPath;
        private final long startTime;
        private final long fileSize;

        public IndexRequest(final Path indexPath, final long startTime, final long fileSize) {
            this.indexPath = indexPath;
            this.startTime = startTime;
            this.fileSize = fileSize;
        }

        public Path getIndexPath() {
            return indexPath;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getFileSize() {
            return fileSize;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        final SSTableIndexIndexer sstableIndexer = new SSTableIndexIndexer(new Configuration());
        for (String arg : args) {
            try {
                sstableIndexer.index(new Path(arg));
            } catch (IOException e) {
                LOG.error("Error indexing {}", arg);
                LOG.error("Error indexing", e);
            }
        }

        sstableIndexer.complete();
    }

    public void complete() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            // No big deal.
        }
        service.shutdown();
        while (true) {
            if (service.isTerminated()) {
                return;
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: hadoop jar /path/to/this/jar com.fullcontact.sstable.index.SSTableIndexIndexer <file-Index.db | directory> [file2.Index.db directory3 ...]");
    }
}