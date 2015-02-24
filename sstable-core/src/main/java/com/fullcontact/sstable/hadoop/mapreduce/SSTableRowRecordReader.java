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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Simple row based record reader.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class SSTableRowRecordReader extends SSTableRecordReader<ByteBuffer, SSTableIdentityIterator> {

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!hasMore()) {
            return false;
        }

        // Read the key and set it.
        final ByteBuffer keyBytes = ByteBufferUtil.readWithShortLength(getReader());
        setCurrentKey(keyBytes);

        // Read the data size.
        // TODO: may have to take into account the fact that files can support long or int depending on Cassandra version.
        final long dataSize = getReader().readLong();

        // Read the value and set it.
        final SSTableIdentityIterator ssTableIdentityIterator = getIdentityIterator(keyBytes, dataSize);
        setCurrentValue(ssTableIdentityIterator);

        return true;
    }

    private SSTableIdentityIterator getIdentityIterator(final ByteBuffer keyBytes, final long dataSize) {
        final DecoratedKey decoratedKey = getDecoratedKey(keyBytes);
        final CFMetaData cfMetaData = getCfMetaData();

        return new SSTableIdentityIterator(cfMetaData, getReader(), getDataPath().toString(), decoratedKey,
            dataSize, false, null, ColumnSerializer.Flag.LOCAL);
    }

    private DecoratedKey getDecoratedKey(final ByteBuffer keyBytes) {
        return getPartitioner().decorateKey(keyBytes);
    }
}
