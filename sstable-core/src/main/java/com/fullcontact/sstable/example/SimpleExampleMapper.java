package com.fullcontact.sstable.example;

import com.google.common.collect.Lists;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
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

    private final AbstractType keyType =
            CompositeType.getInstance(Lists.<AbstractType<?>>newArrayList(UTF8Type.instance, UTF8Type.instance));

    private JsonColumnParser jsonColumnParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.jsonColumnParser = new JsonColumnParser(context.getConfiguration().get(HADOOP_SSTABLE_CQL));
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
