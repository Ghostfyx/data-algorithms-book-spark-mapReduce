package org.dataalgorithms.MBA.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @description:
 *  Market Basket Analysis Algorithm: find the association rule for the list of items
 *  in a basket; That is, there are transaction data in a store:
 *  <ul>
 * <li>apple, cracker, soda, corn </li>
 * <l1>icecream, soda, bread</li>
 * <li>...</li>
 * <ul>
 * The code reads the data as
 * key: first item
 * value: the rest of the items
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-16 16:01
 */
public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final int DEFAULT_NUMBER_OF_PAIRS = 2;

    //output key2: list of items paired; can be 2 or 3 ...
    private static final Text reducerKey = new Text();

    //output value2: number of the paired items in the item list
    private static final IntWritable NUMBER_ONE = new IntWritable(1);

    int numberOfPairs;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] products = value.toString().split(",");
        if ((products == null) || (products.length == 0)) {
            // no mapper output will be generated
            return;
        }
        List<List<String>> sortedCombinations = Combination.findSortedCombinations(Arrays.asList(products), numberOfPairs);
        for (List<String> itemList: sortedCombinations) {
            reducerKey.set(itemList.toString());
            context.write(reducerKey, NUMBER_ONE);
        }
    }

    @Override
    protected void setup(Context context){
        this.numberOfPairs = context.getConfiguration().getInt("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
    }

}
