package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.dataalgorithms.chap02.util.DateUtil;

import java.io.IOException;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-09
 */
public class SecondarySortReducer extends Reducer<CompositeKey, NaturalValue, Text, Text> {

    @Override
    public void reduce(CompositeKey key, Iterable<NaturalValue> values,
                       Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (NaturalValue value : values){
            builder.append("(");
            String dateAsString = DateUtil.getDateAsString(value.getTimestamp());
            double price = value.getPrice();
            builder.append(dateAsString);
            builder.append(",");
            builder.append(price);
            builder.append(")");
        }
        context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
    }

}
