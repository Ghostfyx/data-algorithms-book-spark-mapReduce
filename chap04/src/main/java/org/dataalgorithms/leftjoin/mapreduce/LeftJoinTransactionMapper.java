package org.dataalgorithms.leftjoin.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String productID = tokens[1];
        String userID = tokens[2];
        // make sure products arrive at a reducer after location
        outputKey.set(userID, "2");
        outputValue.set("P", productID);
        context.write(outputKey, outputValue);
    }

}
