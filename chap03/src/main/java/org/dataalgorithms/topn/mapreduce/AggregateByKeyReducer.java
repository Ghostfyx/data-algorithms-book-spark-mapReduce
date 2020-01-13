package org.dataalgorithms.topn.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description: 非唯一键的映射器，例如对网站访问记录数的统计
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class AggregateByKeyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }

}
