package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description: 股票编号与收盘日期的组合键
 *
 * We do a primary grouping pass on the stockSymbol field to get
 * all of the data of one type together, and then our "secondary sort"
 * during the shuffle phase uses the timestamp long member to sort
 * the timeseries points so that they arrive at the reducer partitioned
 * and in sorted order.
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-09 16:52
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    // natural key is (stockSymbol)
    // composite key is a pair (stockSymbol, timestamp)

    private String stockSymbol;

    private Long timestamp;

    public CompositeKey(String stockSymbol, long timestamp) {
        set(stockSymbol, timestamp);
    }

    public CompositeKey() {
    }

    public void set(String stockSymbol, long timestamp) {
        this.stockSymbol = stockSymbol;
        this.timestamp = timestamp;
    }

    public String getStockSymbol() {
        return this.stockSymbol           ;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public int compareTo(CompositeKey o) {
        int natureCompare = this.getStockSymbol().compareTo(o.getStockSymbol());
         if ( natureCompare != 0) {
             return natureCompare;
         }else if (this.timestamp != o.timestamp) {
             return timestamp < o.timestamp ? -1 : 1;
         }
         else {
             return 0;
         }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.stockSymbol);
        dataOutput.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.stockSymbol = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }
}
