package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dataalgorithms.chap02.util.DateUtil;

import java.io.IOException;
import java.util.Date;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-09 18:00
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NaturalValue> {

    // reuse Hadoop's Writable objects
    private final CompositeKey reducerKey = new CompositeKey();
    private final NaturalValue reducerValue = new NaturalValue();

    /**
     * @param key is Hadoop generated, not used here.
     * @param value a record of <stockSymbol><,><Date><,><price>
     */
    @Override
    public void map(LongWritable key,
                    Text value,
                    Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length == 3){
            Date date = DateUtil.getDate(tokens[1]);
            if (date == null) {
                return;
            }
            long timestamp = date.getTime();
            reducerKey.set(tokens[0], timestamp);
            reducerValue.set(timestamp, Double.parseDouble(tokens[2]));
            context.write(reducerKey, reducerValue);
        }
    }

}
