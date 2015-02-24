package com.fullcontact.sstable.example;

import com.fullcontact.sstable.util.CQLUtil;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.fullcontact.sstable.hadoop.mapreduce.HadoopSSTableConstants.HADOOP_SSTABLE_CQL;

/**
 * A simple example demonstrating a possible MapReduce implementation leveraging the sstable reader.
 *
 * Read sstable data and output as text.
 */
public class SimpleExampleMapper extends Mapper<ByteBuffer, SSTableIdentityIterator, Text, Text> {

    private AbstractType keyType = null;

    private JsonColumnParser jsonColumnParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        String cql = context.getConfiguration().get(HADOOP_SSTABLE_CQL);

        if (cql == null || cql.trim().isEmpty()) {
            throw new RuntimeException("Failed CQL create statement empty");
        }

        try {
            final CFMetaData cfm = CQLUtil.parseCreateStatement(cql);

            keyType = cfm.getKeyValidator();
            this.jsonColumnParser = new JsonColumnParser(cfm);
        } catch (RequestValidationException e) {
            throw new RuntimeException("Failed to parse CQL statement: " + cql, e);
        }

    }

    @Override
    protected void map(ByteBuffer key, SSTableIdentityIterator value, Context context)
            throws IOException, InterruptedException {
        final ByteBuffer newBuffer = key.slice();
        final Text mapKey = new Text(keyType.getString(newBuffer));

        Text mapValue = jsonColumnParser.getJson(value, context);
        if (mapValue == null) {
            return;
        }

        context.write(mapKey, mapValue);
    }
}
