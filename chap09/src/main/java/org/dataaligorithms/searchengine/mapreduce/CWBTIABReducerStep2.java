package org.dataaligorithms.searchengine.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Author: FanYueXiang
 * @Date: 2020/2/25 10:02 PM
 */
public class CWBTIABReducerStep2 extends Reducer<Text, Iterable<Tuple2<String, Integer>>,Text, Tuple2<String, Integer>> {

    @Override
    public void reduce(Text key, Iterable<Iterable<Tuple2<String, Integer>>> values, Context context){
        Map<String, Integer> map = new HashMap<>();
        for (Iterable<Tuple2<String, Integer>> items : values){
            for (Tuple2<String, Integer> item : items){
                if (!map.containsKey(item._1)){
                    map.put(item._1, item._2);
                }else {
                    map.replace(item._1, item._2);
                }
            }
        }
        map.forEach((bought2, num)->{
            try {
                context.write(key, new Tuple2<>(bought2,num));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

}
