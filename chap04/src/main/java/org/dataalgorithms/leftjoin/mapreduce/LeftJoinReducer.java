package org.dataalgorithms.leftjoin.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.umd.cloud9.io.pair.PairOfStrings;

import java.io.IOException;
import java.util.Iterator;

/**
 * @description: Iterable<PairOfStrings> values must be: {UserData,ProductData,ProductData,ProductData,ProductData}
 * @author: fanyeuxiang
 * @createDate: 2020-01-13 14:14
 */
public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

    Text productID = new Text();
    Text locationID = new Text("undefined");

    @Override
    public void  reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) throws IOException, InterruptedException {
        Iterator<PairOfStrings> iterator = values.iterator();
        System.out.println("----------debug reduce function begin---------");
        if (iterator.hasNext()){
            // firstPair must be location pair
            PairOfStrings firstPair = iterator.next();
            System.out.println("firstPair="+firstPair.toString());
            if (firstPair.getLeftElement().equals("L")) {
                locationID.set(firstPair.getRightElement());
            }
        }
        while (iterator.hasNext()){
            // the remaining elements must be product pair
            PairOfStrings productPair = iterator.next();
            System.out.println("productPair="+productPair.toString());
            productID.set(productPair.getRightElement());
            context.write(productID, locationID);
        }
        System.out.println("----------debug reduce function begin---------");
    }

}
