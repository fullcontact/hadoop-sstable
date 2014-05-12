package com.fullcontact.cassandra.io.sstable;

/**
 * Cassandra Descriptor ported to work with HDFS. TODO: nuke this.
 * <p/>
 * Subset of the C* version because I don't want the whole thing unless I absolutely need it.
 *
 * @author ben <ben.vanberg@fullcontact.com>
 */
public class SSTable {
    public static final String TEMPFILE_MARKER = "tmp";
}
