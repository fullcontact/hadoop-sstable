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

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils
{
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private static final Method cleanerMethod;

    static
    {
        Method m;
        try
        {
            m = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");
        }
        catch (Exception e)
        {
            // Perhaps a non-sun-derived JVM - contributions welcome
            logger.info("Cannot initialize un-mmaper.  (Are you using a non-SUN JVM?)  Compacted data files will not be removed promptly.  Consider using a SUN JVM or using standard disk access mode");
            m = null;
        }
        cleanerMethod = m;
    }

    public static void closeQuietly(Closeable c)
    {
        try
        {
            if (c != null)
                c.close();
        }
        catch (Exception e)
        {
            logger.warn("Failed closing " + c, e);
        }
    }

    public static boolean isCleanerAvailable()
    {
        return cleanerMethod != null;
    }

    public static void clean(MappedByteBuffer buffer)
    {
        try
        {
            Object cleaner = cleanerMethod.invoke(buffer);
            cleaner.getClass().getMethod("clean").invoke(cleaner);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
        catch (NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void skipBytesFully(DataInput in, int bytes) throws IOException
    {
        int n = 0;
        while (n < bytes)
        {
            int skipped = in.skipBytes(bytes - n);
            if (skipped == 0)
                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
            n += skipped;
        }
    }
}
