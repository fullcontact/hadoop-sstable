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

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class SSTableIndexIndexTest {
    private static final String INDEX_FILE = "/data/Keyspace1-Standard1-ic-0-Index.db";

    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        fileSystem = FileSystem.getLocal(new Configuration());
    }

    @After
    public void tearDown() throws Exception {
        fileSystem.close();
    }

    @Test
    public void testCreateAndReadIndex() throws Exception {

        // Create a temp dir...
        final File tmpDir = Files.createTempDir();
        tmpDir.deleteOnExit();

        // Copy the .Index.db file to the temp dir...
        File sampleFile = new File(getClass().getResource(INDEX_FILE).getFile());
        File tmpSampleFile = new File(tmpDir, sampleFile.getName());
        Files.copy(sampleFile, tmpSampleFile);

        // Create the file...
        fileSystem.getConf().set("hadoop.sstable.split.mb", "1");
        SSTableIndexIndex.writeIndex(fileSystem, new Path(tmpDir.getAbsolutePath(), sampleFile.getName()));

        // Validate that the file was created...
        SSTableIndexIndex index = SSTableIndexIndex.readIndex(fileSystem,
                new Path(tmpDir.getAbsolutePath(), sampleFile.getName()));

        assertNotNull(index);
        assertEquals(1, index.getOffsets().size());
        assertEquals(0, index.getOffsets().get(0).getStart());
        assertEquals(18, index.getOffsets().get(0).getEnd());
    }
}
