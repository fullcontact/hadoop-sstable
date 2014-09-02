package com.fullcontact.sstable.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * A simple example demonstrating a possible MapReduce implementation leveraging the sstable reader.
 *
 * Read sstable data and output as text.
 */
public class SimpleExampleReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Make things super simple and output the first value only. In reality we'd want to figure out which was the
        // most correct value of the ones we have based on our C* cluster configuration.
        context.write(key, new Text(values.iterator().next().toString()));
    }
}
