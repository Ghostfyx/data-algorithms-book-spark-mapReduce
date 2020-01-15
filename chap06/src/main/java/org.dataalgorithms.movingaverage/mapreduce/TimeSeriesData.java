package org.dataalgorithms.movingaverage.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-15
 */
public class TimeSeriesData implements WritableComparable<TimeSeriesData> {

    private long timeStamp;

    private double value;

    public TimeSeriesData(){}

    public TimeSeriesData(long timeStamp, double value) {
        this.timeStamp = timeStamp;
        this.value = value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public int compareTo(TimeSeriesData o) {
        if (this.timeStamp  < o.timeStamp ) {
            return -1;
        }
        else if (this.timeStamp  > o.timeStamp ) {
            return 1;
        }
        else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timeStamp);
        dataOutput.writeDouble(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timeStamp = dataInput.readLong();
        this.value = dataInput.readDouble();
    }
}
