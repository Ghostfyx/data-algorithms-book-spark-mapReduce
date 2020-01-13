package org.dataalgorithms.topn.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @description:
 *  Reducer's input are local top N from all mappers.
 *  We have a single reducer, which creates the final top N.
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-10 9:14
 */
public class TopNReducer extends Reducer<NullWritable, Text, DoubleWritable, Text> {

    private int N = 10;

    private SortedMap<Double, String> topCats = new TreeMap<Double, String>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t : values){
            String valueAsString = t.toString().trim();
            String[] tokens = valueAsString.split(",");
            double weight = Double.parseDouble(tokens[0]);
            String cat = tokens[1] + "-" + tokens[2];
            topCats.put(weight, cat);
            // keep only top N
            if (topCats.size() > N) {
                topCats.remove(topCats.firstKey());
            }
        }
       for (double weight : topCats.keySet()){
            context.write(new DoubleWritable(weight), new Text(topCats.get(weight)));
       }
    }

    @Override
    protected void setup(Context context){
        this.N = context.getConfiguration().getInt("N", 10); // default is top 10
    }

}
