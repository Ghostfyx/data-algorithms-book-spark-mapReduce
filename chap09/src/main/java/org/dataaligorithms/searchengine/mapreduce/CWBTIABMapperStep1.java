package org.dataaligorithms.searchengine.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description: 该买过该商品的用户还购买了哪些商品(CWBTIAB)MapReduce实现，使用Stripes设计模式，第一阶段
 * mapper，输入：<user><,><bought_item> 输出：<user><bought_item>
 * @Author: FanYueXiang
 * @Date: 2020/2/25 8:58 PM
 */
public class CWBTIABMapperStep1 extends Mapper<LongWritable,Text,Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String user = value.toString().split("，")[0];
        String boughtItems = value.toString().split("，")[1];
        context.write(new Text(user), new Text(boughtItems));
    }

}
