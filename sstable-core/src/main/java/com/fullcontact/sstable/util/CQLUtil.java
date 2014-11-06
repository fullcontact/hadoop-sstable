package com.fullcontact.sstable.util;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateColumnFamilyStatement;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * Basic utilities, centralizing CQL convenience functions
 *
 * @author Michael Rose <michael@fullcontact.com>
 */
public class CQLUtil {
    /**
     * Parses a CQL CREATE statement into its CFMetaData object
     *
     * @param cql
     * @return CFMetaData
     * @throws RequestValidationException if CQL is invalid
     */
    public static CFMetaData parseCreateStatement(String cql) throws RequestValidationException {
        final CreateColumnFamilyStatement statement =
                (CreateColumnFamilyStatement) QueryProcessor.parseStatement(cql).prepare().statement;

        final CFMetaData cfm =
                new CFMetaData("assess", "kvs_strict", ColumnFamilyType.Standard, statement.comparator, null);

        statement.applyPropertiesTo(cfm);
        return cfm;
    }

    private CQLUtil() {}
}
