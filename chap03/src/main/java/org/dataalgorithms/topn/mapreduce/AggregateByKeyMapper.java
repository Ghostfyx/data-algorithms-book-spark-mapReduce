package org.dataalgorithms.topn.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description: 非唯一键的映射器，例如对网站访问记录数的统计
 * @author: fanyeuxiang
 * @createDate: 2020-01-10
 */
public class AggregateByKeyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text K2 = new Text();

    private IntWritable V2 = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length != 2) {
            return;
        }
        String url = tokens[0];
        int frequency =  Integer.parseInt(tokens[1]);
        K2.set(url);
        V2.set(frequency);
        context.write(K2, V2);
    };

}
