package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description: 自然键分区
 * @author: fanyeuxiang
 * @createDate: 2020-01-09 17:56
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NaturalValue> {
    @Override
    public int getPartition(CompositeKey compositeKey, NaturalValue naturalValue, int numberOfPartitions) {
        return Math.abs((int) (hash(compositeKey.getStockSymbol()) % numberOfPartitions));
    }

    /**
     *  adapted from String.hashCode()
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
