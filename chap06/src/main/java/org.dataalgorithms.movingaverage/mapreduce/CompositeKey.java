package org.dataalgorithms.movingaverage.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description: 二次排序组合键
 *
 * CompositeKey: represents a pair of
 * (String name, long timestamp).
 *
 * We do a primary grouping pass on the name field to get all of the data of
 * one type together, and then our "secondary sort" during the shuffle phase
 * uses the timestamp long member to sort the timeseries points so that they
 * arrive at the reducer partitioned and in sorted order.
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-15 16:14
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    private String name;

    private long timeStamp;

    public CompositeKey(){

    }

    public CompositeKey(String name, long timeStamp) {
        this.name = name;
        this.timeStamp = timeStamp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public int compareTo(CompositeKey o) {
        int nameCompare = this.getName().compareToIgnoreCase(o.getName());
        if (nameCompare != 0) {
            return nameCompare;
        }else {
            long timeStrampCompare = this.getTimeStamp() - o.getTimeStamp();
            if (timeStrampCompare == 0){
                return 0;
            }else {
                return timeStrampCompare < 0 ? -1 : 1;
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.name);
        dataOutput.writeLong(this.timeStamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.timeStamp = dataInput.readLong();
    }
}
