package org.dataalgorithms.leftjoin.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description: 左连接第二阶段Mapper
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class LocationCountMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text  key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
