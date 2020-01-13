package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description: 自然值
 * @author: fanyeuxiang
 * @createDate: 2020-01-09 17:36
 */
public class NaturalValue implements Writable, Comparable<NaturalValue> {

    private long timestamp;

    private double price;

    public NaturalValue(long timestamp, double price) {
        set(timestamp, price);
    }

    public NaturalValue() {
    }

    public void set(long timestamp, double price) {
        this.timestamp = timestamp;
        this.price = price;
    }

    @Override
    public int compareTo(NaturalValue data) {
        if (this.timestamp  < data.timestamp ) {
            return -1;
        }
        else if (this.timestamp  > data.timestamp ) {
            return 1;
        }
        else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timestamp);
        dataOutput.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.price = dataInput.readDouble();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getPrice() {
        return price;
    }
}
