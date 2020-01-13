package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @description: 组合键比较器
 * @author: fanyeuxiang
 * @createDate: 2020-01-09
 */
public class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        CompositeKey ck1 = (CompositeKey) wc1;
        CompositeKey ck2 = (CompositeKey) wc2;
        int comparison = ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
        if (comparison == 0) {
            // stock symbols are equal here
            if (ck1.getTimestamp() == ck2.getTimestamp()) {
                return 0;
            }
            else if (ck1.getTimestamp() < ck2.getTimestamp()) {
                return -1;
            }
            else {
                return 1;
            }
        }
        else {
            return comparison;
        }
    }

}
