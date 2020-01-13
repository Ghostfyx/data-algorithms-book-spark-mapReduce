package org.dataalgorithms.leftjoin.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

/**
 * @description: The SecondarySortGroupComparator class indicates how to compare the userIDs.
 *
 * RawComparator:对对象的字节表示进行比较的接口
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-13 14:03
 */
public class SecondarySortGroupComparator implements RawComparator<PairOfStrings> {
    @Override
    public int compare(byte[] b1, int i, int i1, byte[] b2, int i2, int i3) {
        DataInputBuffer buffer = new DataInputBuffer();
        PairOfStrings a = new PairOfStrings();
        PairOfStrings b = new PairOfStrings();
        try {
            buffer.reset(b1, i, i1);
            a.readFields(buffer);
            buffer.reset(b2, i2, i3);
            b.readFields(buffer);
            return compare(a,b);
        }
        catch(Exception ex) {
            return -1;
        }
    }

    @Override
    public int compare(PairOfStrings o1, PairOfStrings o2) {
        return o1.getLeftElement().compareTo(o2.getLeftElement());
    }
}
