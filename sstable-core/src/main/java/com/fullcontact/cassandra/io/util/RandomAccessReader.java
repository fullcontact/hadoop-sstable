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

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Cassandra RandomAccessReader ported to work with HDFS.
 */
public class RandomAccessReader implements FileDataInput {
    public static final long CACHE_FLUSH_INTERVAL_IN_BYTES = (long) Math.pow(2, 27); // 128mb

    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65536;

    // absolute filesystem path to the file
    private final String filePath;
    protected final FSDataInputStream input;
    private final Path inputPath;
    private final FileStatus inputFileStatus;

    // buffer which will cache file blocks
    protected byte[] buffer;

    // `current` as current position in file
    // `bufferOffset` is the offset of the beginning of the buffer
    // `markedPointer` folds the offset of the last file mark
    protected long bufferOffset, current = 0, markedPointer;
    // `validBufferBytes` is the number of bytes in the buffer that are actually valid;
    //  this will be LESS than buffer capacity if buffer is not full!
    protected int validBufferBytes = 0;

    private final boolean skipIOCache;

    // used if skip I/O cache was enabled
    private long bytesSinceCacheFlush = 0;

    private final long fileLength;

    protected final PoolingSegmentedFile owner;

    private final FileSystem fs;

    protected RandomAccessReader(Path file, int bufferSize, boolean skipIOCache, PoolingSegmentedFile owner, FileSystem fs) throws FileNotFoundException {
        inputPath = file;
        try {
            inputFileStatus = fs.getFileStatus(inputPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.fs = fs;

        try {
            this.input = fs.open(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.owner = owner;

        filePath = file.toString();

        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");
        buffer = new byte[bufferSize];

        this.skipIOCache = skipIOCache;

        // we can cache file length in read-only mode
        try {
            fileLength = fs.getFileStatus(file).getLen();
        } catch (IOException e) {
            throw new FSReadError(e, filePath);
        }
        validBufferBytes = -1; // that will trigger reBuffer() on demand by read/seek operations
    }

    public static RandomAccessReader open(Path file, FileSystem fs) {
        return open(file, false, fs);
    }

    public static RandomAccessReader open(Path file, PoolingSegmentedFile owner, FileSystem fs) {
        return open(file, DEFAULT_BUFFER_SIZE, false, owner, fs);
    }

    public static RandomAccessReader open(Path file, boolean skipIOCache, FileSystem fs) {
        return open(file, DEFAULT_BUFFER_SIZE, skipIOCache, null, fs);
    }

    @VisibleForTesting
    static RandomAccessReader open(Path file, int bufferSize, boolean skipIOCache, PoolingSegmentedFile owner, FileSystem fs) {
        try {
            return new RandomAccessReader(file, bufferSize, skipIOCache, owner, fs);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static RandomAccessReader open(SequentialWriter writer, FileSystem fs) {
        return open(new Path(writer.getPath()), DEFAULT_BUFFER_SIZE, false, null, fs);
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    protected void reBuffer() {
        resetBuffer();

        try {
            if (bufferOffset >= fs.getFileStatus(inputPath).getLen()) // TODO: is this equivalent?
                return;

            input.seek(bufferOffset);

            int read = 0;

            while (read < buffer.length) {
                int n = input.read(buffer, read, buffer.length - read);
                if (n < 0)
                    break;
                read += n;
            }

            validBufferBytes = read;
            bytesSinceCacheFlush += read;
        } catch (IOException e) {
            throw new FSReadError(e, filePath);
        }
    }

    @Override
    public long getFilePointer() {
        return current;
    }

    public String getPath() {
        return filePath;
    }

    public void reset() {
        seek(markedPointer);
    }

    public long bytesPastMark() {
        long bytes = current - markedPointer;
        assert bytes >= 0l;
        return bytes;
    }

    public FileMark mark() {
        markedPointer = current;
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark) {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(FileMark mark) {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF() {
        return getFilePointer() == length();
    }

    public long bytesRemaining() {
        return length() - getFilePointer();
    }

    protected int bufferCursor() {
        return (int) (current - bufferOffset);
    }

    protected void resetBuffer() {
        bufferOffset = current;
        validBufferBytes = 0;
    }

    @Override
    public void close() {
        if (owner == null || buffer == null) {
            // The buffer == null check is so that if the pool owner has deallocated us, calling close()
            // will re-call deallocate rather than recycling a deallocated object.
            // I'd be more comfortable if deallocate didn't have to handle being idempotent like that,
            // but RandomAccessFile.close will call AbstractInterruptibleChannel.close which will
            // re-call RAF.close -- in this case, [C]RAR.close since we are overriding that.
            deallocate();
        } else {
            owner.recycle(this);
        }
    }

    public void deallocate() {
        buffer = null; // makes sure we don't use this after it's ostensibly closed

        try {
            input.close();
        } catch (IOException e) {
            throw new FSReadError(e, filePath);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + "filePath='" + filePath + "'" + ", skipIOCache=" + skipIOCache + ")";
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int n = 0;
        do {
            int count = this.read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        } while (n < len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        long pos;
        long len;
        long newpos;

        if (n <= 0) {
            return 0;
        }
        pos = getFilePointer();
        len = length();
        newpos = pos + n;
        if (newpos > len) {
            newpos = len;
        }
        seek(newpos);

        /* return the actual number of bytes skipped */
        return (int) (newpos - pos);
    }

    @Override
    public boolean readBoolean() throws IOException {
        int ch = this.read();
        if (ch < 0)
            throw new EOFException();
        return (ch != 0);
    }

    @Override
    public byte readByte() throws IOException {
        int ch = this.read();
        if (ch < 0)
            throw new EOFException();
        return (byte) (ch);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        int ch = this.read();
        if (ch < 0)
            throw new EOFException();
        return ch;
    }

    @Override
    public short readShort() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short) ((ch1 << 8) + (ch2 << 0));
    }

    @Override
    public int readUnsignedShort() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (ch1 << 8) + (ch2 << 0);
    }

    @Override
    public char readChar() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char) ((ch1 << 8) + (ch2 << 0));
    }

    @Override
    public int readInt() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        int ch3 = this.read();
        int ch4 = this.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    @Override
    public long readLong() throws IOException {
        return ((long) (readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readLine() throws IOException {
        StringBuffer input = new StringBuffer();
        int c = -1;
        boolean eol = false;

        while (!eol) {
            switch (c = read()) {
                case -1:
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = getFilePointer();
                    if ((read()) != '\n') {
                        seek(cur);
                    }
                    break;
                default:
                    input.append((char) c);
                    break;
            }
        }

        if ((c == -1) && (input.length() == 0)) {
            return null;
        }
        return input.toString();
    }

    @Override
    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements FileMark {
        final long pointer;

        public BufferedRandomAccessFileMark(long pointer) {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition) {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (newPosition > length()) // it is save to call length() in read-only mode
            throw new IllegalArgumentException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                    newPosition, getPath(), length()));

        current = newPosition;

        if (newPosition > (bufferOffset + validBufferBytes) || newPosition < bufferOffset)
            reBuffer();
    }

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read() {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (current >= bufferOffset + buffer.length || validBufferBytes == -1)
            reBuffer();

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        return ((int) buffer[(int) (current++ - bufferOffset)]) & 0xff;
    }

    public int read(byte[] buffer) {
        return read(buffer, 0, buffer.length);
    }

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length) {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (current >= bufferOffset + buffer.length || validBufferBytes == -1)
            reBuffer();

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes
                : String.format("File (%s), current offset %d, buffer offset %d, buffer limit %d",
                getPath(),
                current,
                bufferOffset,
                validBufferBytes);

        int toCopy = Math.min(length, validBufferBytes - bufferCursor());

        System.arraycopy(buffer, bufferCursor(), buff, offset, toCopy);
        current += toCopy;

        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws EOFException {
        assert length >= 0 : "buffer length should not be negative: " + length;

        byte[] buff = new byte[length];

        try {
            readFully(buff); // reading data buffer
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            throw new FSReadError(e, filePath);
        }

        return ByteBuffer.wrap(buff);
    }

    public long length() {
        return fileLength;
    }

    public void write(int value) {
        throw new UnsupportedOperationException();
    }

    public void write(byte[] buffer) {
        throw new UnsupportedOperationException();
    }

    public void write(byte[] buffer, int offset, int length) {
        throw new UnsupportedOperationException();
    }
}
