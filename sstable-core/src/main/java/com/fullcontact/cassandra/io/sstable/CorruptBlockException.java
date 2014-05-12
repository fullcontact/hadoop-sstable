package com.fullcontact.cassandra.io.sstable;

import com.fullcontact.cassandra.io.compress.CompressionMetadata;

import java.io.IOException;

public class CorruptBlockException extends IOException {
    public CorruptBlockException(String filePath, CompressionMetadata.Chunk chunk) {
        this(filePath, chunk.offset, chunk.length);
    }

    public CorruptBlockException(String filePath, long offset, int length) {
        super(String.format("(%s): corruption detected, chunk at %d of length %d.", filePath, offset, length));
    }
}
