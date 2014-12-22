package com.fullcontact.sstable.index;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongWritablePair implements Writable {

    private long l1;
    private long l2;

    public LongWritablePair() {
    }

    public LongWritablePair(long l1, long l2) {
        this.l1 = l1;
        this.l2 = l2;
    }

    public void set_1(long l1) {
        this.l1 = l1;
    }

    public void set_2(long l2) {
        this.l2 = l2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(l1);
        dataOutput.writeLong(l2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        l1 = dataInput.readLong();
        l2 = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "LongWritablePair{" +
                "l1=" + l1 +
                ", l2=" + l2 +
                '}';
    }
}
