package org.dataalgorithms.movingaverage.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @description: combiner
 *
 * This class is used during Hadoop's shuffle phase to
 * group composite key's by the first part (natural) of
 * their key.
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-15
 */
public class CompositeKeyGroupingComparator extends WritableComparator {

    protected CompositeKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        return key1.getName().compareTo(key2.getName());
    }

}
