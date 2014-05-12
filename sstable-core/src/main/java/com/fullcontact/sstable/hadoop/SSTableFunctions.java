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

package com.fullcontact.sstable.hadoop;

import com.google.common.base.Function;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

/**
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class SSTableFunctions {

    public static final Function<Path, Path> INDEX_FILE = indexFile();

    /**
     * Return a function which determines the SSTable index file when supplied with the SSTable data file.
     * @return Function.
     */
    public static Function<Path, Path> indexFile() {
        return new Function<Path, Path>() {
            @Nullable
            @Override
            public Path apply(@Nullable Path dataFile) {
                final String dataFileName = dataFile.getName();
                return new Path(dataFile.getParent(), dataFileName.replace("-Data.db", "-Index.db"));
            }
        };
    }
}
