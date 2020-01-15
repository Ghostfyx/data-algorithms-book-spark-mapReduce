package org.dataalgorithms.movingaverage.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dataalgorithms.movingaverage.util.DateUtil;

import java.io.IOException;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-15 16:45
 */
public class SortByMRF_MovingAverageMapper extends Mapper<LongWritable, Text, CompositeKey,TimeSeriesData> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length != 3){
            throw new IllegalArgumentException("input file line must be :<name><,><time><,><price>");
        }
        String name = tokens[0];
        String date = tokens[1];
        long timeStamp =DateUtil.getDate(date).getTime();
        double price = Double.parseDouble(tokens[2]);
        CompositeKey compositeKey = new CompositeKey(name, timeStamp);
        TimeSeriesData timeSeriesData = new TimeSeriesData(timeStamp, price);
        context.write(compositeKey, timeSeriesData);
    }

}
