package org.algorithms.orderinversion.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description: 反转排序根据自然键分区，自然键相同的映射到一个Reducer
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {

    @Override
    public int getPartition(PairOfWords key, IntWritable value, int numPartitions) {
        String leftWord = key.getLeftWord();
        return Math.abs(((int) leftWord.hashCode()) % numPartitions);
    }
}
