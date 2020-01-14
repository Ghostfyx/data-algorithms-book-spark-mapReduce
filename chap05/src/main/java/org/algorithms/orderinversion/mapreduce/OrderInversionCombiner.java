package org.algorithms.orderinversion.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @descriptio: combiner在Map输出之后聚合结果，提升Reducer执行效率
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class OrderInversionCombiner extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable> {

    @Override
    public void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int partionSum = 0;
        for (IntWritable value : values){
            partionSum += value.get();
        }
        context.write(key, new IntWritable(partionSum));
    }
}
