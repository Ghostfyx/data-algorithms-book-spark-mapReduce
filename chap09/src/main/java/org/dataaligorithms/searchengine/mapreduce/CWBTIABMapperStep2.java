package org.dataaligorithms.searchengine.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description: 该买过该商品的用户还购买了哪些商品(CWBTIAB)MapReduce实现，使用Stripes设计模式，第二阶段
 * mapper，输入：<user><,><bought_item> 输出：<user>{<bought_item>,<bought_item_num>}
 * @Author: FanYueXiang
 * @Date: 2020/2/25 9:35 PM
 */
public class CWBTIABMapperStep2 extends Mapper<Text,Iterable<Text>,Text, Tuple2<String, Integer>> {

    @Override
    public void map(Text key, Iterable<Text> values, Context context){
        for (Text bought: values){
            String[] items = bought.toString().split(" ");
            Map<String, Integer> map = new HashMap<>();
            for (String item : items){
                if (!map.containsKey(item)){
                    map.put(item, 1);
                }else {
                    map.replace(item, map.get(item) + 1);
                }
            }
            map.forEach((String bought2, Integer num) ->{
                Tuple2 tuple2 = new Tuple2(bought2,num);
                try {
                    context.write(bought, tuple2);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }

}
