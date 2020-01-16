package org.dataalgorithms.MBA.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-16
 */
public class MBAReducer extends Reducer<Text, IntWritable, Text, IntWritable > {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0; // total items paired
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }

}
