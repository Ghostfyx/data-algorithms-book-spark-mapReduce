package org.dataalgorithms.topn.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-10 18:29
 */
public class AggregateByKeyMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context){

    };

}
