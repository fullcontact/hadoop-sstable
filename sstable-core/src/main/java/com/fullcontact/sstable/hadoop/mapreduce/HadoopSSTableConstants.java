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

/**
 * @author ben <ben.vanberg@fullcontact.com>
 */
public interface HadoopSSTableConstants {

    /**
     * CQL SSTable create statement. This will define the column family metadata.
     */
    public static final String HADOOP_SSTABLE_CQL = "hadoop.sstable.cql";

    /**
     * SSTable key space.
     */
    public static final String HADOOP_SSTABLE_KEYSPACE = "hadoop.sstable.keyspace";

    /**
     * SSTable column family name.
     */
    public static final String HADOOP_SSTABLE_COLUMN_FAMILY_NAME = "hadoop.sstable.column.family.name";

    /**
     * Default SSTable split size.
     */
    public static final int DEFAULT_SPLIT_MB = 1024;

    /**
     * SSTable split size in MB.
     */
    public static final String HADOOP_SSTABLE_SPLIT_MB = "hadoop.sstable.split.mb";
}
