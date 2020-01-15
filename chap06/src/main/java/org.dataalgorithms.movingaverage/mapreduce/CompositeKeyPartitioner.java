package org.dataalgorithms.movingaverage.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-15
 */
public class CompositeKeyPartitioner extends Partitioner<CompositeKey, TimeSeriesData> {
    @Override
    public int getPartition(CompositeKey key, TimeSeriesData value, int numPartitions) {
        return Math.abs((int)(hash(key.getName()) % numPartitions));
    }

    /**
     * adapted from String.hashCode()
     */
    static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }
}
