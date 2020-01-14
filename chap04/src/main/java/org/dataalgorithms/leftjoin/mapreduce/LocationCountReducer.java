package org.dataalgorithms.leftjoin.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class LocationCountReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text productID, Iterable<Text> locations, Context context) throws IOException, InterruptedException {
        Set<String> locationSet = new HashSet<>();
        for (Text location: locations) {
            locationSet.add(location.toString());
        }
        context.write(productID, new IntWritable(locationSet.size()));
    }

}
