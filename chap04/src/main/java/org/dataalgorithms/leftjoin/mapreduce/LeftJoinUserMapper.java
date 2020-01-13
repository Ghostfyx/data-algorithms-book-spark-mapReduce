package org.dataalgorithms.leftjoin.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description: Spark 左外链接读取User数据
 * @author: fanyeuxiang
 * @createDate: 2020-01-13 11:03
 */
public class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("value:"+value.toString());
        String[] tokens = value.toString().split(",");
        System.out.println("value-split:"+tokens[0]);
        String userId = tokens[0];
        String localtionId = tokens[1];
        outputKey.set(userId, "1");
        // set location_id
        outputValue.set("L", localtionId);
        context.write(outputKey, outputValue);
    }
}
