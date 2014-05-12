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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.text.DecimalFormat;

/**
 * Cassandra FileUtils ported to work with HDFS.
 */
public class FileUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private static final double KB = 1024d;
    private static final double MB = 1024 * 1024d;
    private static final double GB = 1024 * 1024 * 1024d;
    private static final double TB = 1024 * 1024 * 1024 * 1024d;

    private static final DecimalFormat df = new DecimalFormat("#.##");

    private static final Method cleanerMethod;

    static {
        Method m;
        try {
            m = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");
        } catch (Exception e) {
            // Perhaps a non-sun-derived JVM - contributions welcome
            logger.info("Cannot initialize un-mmaper.  (Are you using a non-SUN JVM?)  Compacted data files will not be removed promptly.  Consider using a SUN JVM or using standard disk access mode");
            m = null;
        }
        cleanerMethod = m;
    }

//    public static void createHardLink(Path from, Path to, FileSystem fs)
//    {
//        try {
//            if (fs.exists(to))
//                throw new RuntimeException("Tried to create duplicate hard link to " + to);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        try {
//            if (!fs.exists(from))
//                throw new RuntimeException("Tried to hard link to file that does not exist " + from);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        try
//        {
//            CLibrary.createHardLink(from, to);
//        }
//        catch (IOException e)
//        {
//            throw new FSWriteError(e, to);
//        }
//    }

//    public static Path createTempFile(String prefix, String suffix, Path directory, FileSystem fs)
//    {
//        try
//        {
//            Path path = new Path(directory, prefix + suffix);
//            boolean result = fs.createNewFile(path);
//            return path;
////            return File.createTempFile(prefix, suffix, directory);
//        }
//        catch (IOException e)
//        {
//            throw new FSWriteError(e, directory);
//        }
//    }

//    public static Path createTempFile(String prefix, String suffix, FileSystem fs)
//    {
//        return createTempFile(prefix, suffix, new Path(System.getProperty("java.io.tmpdir")), fs);
//    }

//    public static void deleteWithConfirm(String file, FileSystem fs, boolean recursive)
//    {
//        deleteWithConfirm(new Path(file), fs, recursive);
//    }

//    public static void deleteWithConfirm(Path file, FileSystem fs, boolean recursive)
//    {
//        try {
//            assert fs.exists(file) : "attempted to delete non-existing file " + file.getName();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        if (logger.isDebugEnabled())
//            logger.debug("Deleting " + file.getName());
//        try {
//            if (!fs.delete(file, recursive))
//                throw new FSWriteError(new IOException("Failed to delete " + file.getParent()), file);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        } catch (FSWriteError fsWriteError) {
//            fsWriteError.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//    }

//    public static void renameWithOutConfirm(String from, String to, FileSystem fs)
//    {
//        try {
//            fs.rename(new Path(from), new Path(to));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
////        new Path(from).renameTo(new Path(to));
//    }
//
//    public static void renameWithConfirm(String from, String to, FileSystem fs)
//    {
//        renameWithConfirm(new Path(from), new Path(to), fs);
//    }

//    public static void renameWithConfirm(Path from, Path to, FileSystem fs)
//    {
//        try {
//            assert fs.exists(from);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        if (logger.isDebugEnabled())
//            logger.debug((String.format("Renaming %s to %s", from, to)));
//        // this is not FSWE because usually when we see it it's because we didn't close the file before renaming it,
//        // and Windows is picky about that.
//        try {
//            if (!fs.rename(from, to))
//                throw new RuntimeException(String.format("Failed to rename %s to %s", from, to));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

//    public static void truncate(String path, long size)
//    {
//        RandomAccessFile file;
//
//        try
//        {
//            file = new RandomAccessFile(path, "rw");
//        }
//        catch (FileNotFoundException e)
//        {
//            throw new RuntimeException(e);
//        }
//
//        try
//        {
//            file.getChannel().truncate(size);
//        }
//        catch (IOException e)
//        {
//            throw new FSWriteError(e, path);
//        }
//        finally
//        {
//            closeQuietly(file);
//        }
//    }

    public static void closeQuietly(Closeable c) {
        try {
            if (c != null)
                c.close();
        } catch (Exception e) {
            logger.warn("Failed closing " + c, e);
        }
    }

//    public static void close(Closeable... cs) throws IOException
//    {
//        close(Arrays.asList(cs));
//    }

    //    public static void close(Iterable<? extends Closeable> cs) throws IOException
//    {
//        IOException e = null;
//        for (Closeable c : cs)
//        {
//            try
//            {
//                if (c != null)
//                    c.close();
//            }
//            catch (IOException ex)
//            {
//                e = ex;
//                logger.warn("Failed closing stream " + c, ex);
//            }
//        }
//        if (e != null)
//            throw e;
//    }
//
//    public static String getCanonicalPath(String filename)
//    {
//        try
//        {
//            return new Path(filename).toString();
//        }
//        catch (IOException e)
//        {
//            throw new FSReadError(e, filename);
//        }
//    }
//
//    public static String getCanonicalPath(Path file)
//    {
//        try
//        {
//            return file.toString();
//        }
//        catch (IOException e)
//        {
//            throw new FSReadError(e, file);
//        }
//    }
//
    public static boolean isCleanerAvailable() {
        return cleanerMethod != null;
    }

    public static void clean(MappedByteBuffer buffer) {
        try {
            Object cleaner = cleanerMethod.invoke(buffer);
            cleaner.getClass().getMethod("clean").invoke(cleaner);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
//
//    public static void createDirectory(String directory, FileSystem fs)
//    {
//        createDirectory(new Path(directory), fs);
//    }

//    public static void createDirectory(Path directory, FileSystem fs)
//    {
//        try {
//            if (!fs.exists(directory))
//            {
//                if (!fs.mkdirs(directory))
//                    throw new FSWriteError(new IOException("Failed to mkdirs " + directory), directory);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        } catch (FSWriteError fsWriteError) {
//            fsWriteError.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//    }
//
//    public static boolean delete(String file, FileSystem fs)
//    {
//        Path f = new Path(file);
//        try {
//            return fs.delete(f, false);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public static void delete(Path[] files, FileSystem fs)
//    {
//        for ( Path file : files )
//        {
//            try {
//                fs.delete(file, false);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }
//
//    public static void deleteAsync(final String file, final FileSystem fs, final boolean recursive)
//    {
//        Runnable runnable = new Runnable()
//        {
//            public void run()
//            {
//                deleteWithConfirm(new Path(file), fs, recursive);
//            }
//        };
//        StorageService.tasks.execute(runnable);
//    }
//
//    public static String stringifyFileSize(double value)
//    {
//        double d;
//        if ( value >= TB )
//        {
//            d = value / TB;
//            String val = df.format(d);
//            return val + " TB";
//        }
//        else if ( value >= GB )
//        {
//            d = value / GB;
//            String val = df.format(d);
//            return val + " GB";
//        }
//        else if ( value >= MB )
//        {
//            d = value / MB;
//            String val = df.format(d);
//            return val + " MB";
//        }
//        else if ( value >= KB )
//        {
//            d = value / KB;
//            String val = df.format(d);
//            return val + " KB";
//        }
//        else
//        {
//            String val = df.format(value);
//            return val + " bytes";
//        }
//    }
//
//    /**
//     * Deletes all files and subdirectories under "dir".
//     * @param dir Directory to be deleted
//     * @throws FSWriteError if any part of the tree cannot be deleted
//     */
//    public static void deleteRecursive(Path dir, FileSystem fs)
//    {
////        try {
////            if (fs.getFileStatus(dir).isDir())
////            {
////                String[] children = dir.list();
////                for (String child : children)
////                    deleteRecursive(new File(dir, child));
////            }
////        } catch (IOException e) {
////            throw new RuntimeException(e);
////        }
//
//        // The directory is now empty so now it can be smoked
//        deleteWithConfirm(dir, fs, true);
//    }

    public static void skipBytesFully(DataInput in, int bytes) throws IOException {
        int n = 0;
        while (n < bytes) {
            int skipped = in.skipBytes(bytes - n);
            if (skipped == 0)
                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
            n += skipped;
        }
    }

//    public static void skipBytesFully(DataInput in, long bytes) throws IOException
//    {
//        long n = 0;
//        while (n < bytes)
//        {
//            int m = (int) Math.min(Integer.MAX_VALUE, bytes - n);
//            int skipped = in.skipBytes(m);
//            if (skipped == 0)
//                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
//            n += skipped;
//        }
//    }

    // TODO: Hack this tentacle off as I think it only makes sense with a running cluster.
//    public static void handleFSError(FSError e)
//    {
//        switch (DatabaseDescriptor.getDiskFailurePolicy())
//        {
//            case stop:
//                if (StorageService.instance.isInitialized())
//                {
//                    logger.error("Stopping gossiper");
//                    StorageService.instance.stopGossiping();
//                }
//
//                if (StorageService.instance.isRPCServerRunning())
//                {
//                    logger.error("Stopping RPC server");
//                    StorageService.instance.stopRPCServer();
//                }
//
//                if (StorageService.instance.isNativeTransportRunning())
//                {
//                    logger.error("Stopping native transport");
//                    StorageService.instance.stopNativeTransport();
//                }
//                break;
//            case best_effort:
//                // for both read and write errors mark the path as unwritable.
//                BlacklistedDirectories.maybeMarkUnwritable(e.path);
//                if (e instanceof FSReadError)
//                {
//                    File directory = BlacklistedDirectories.maybeMarkUnreadable(e.path);
//                    if (directory != null)
//                        Table.removeUnreadableSSTables(directory);
//                }
//                break;
//            case ignore:
//                // already logged, so left nothing to do
//                break;
//            default:
//                throw new IllegalStateException();
//        }
//    }
}
