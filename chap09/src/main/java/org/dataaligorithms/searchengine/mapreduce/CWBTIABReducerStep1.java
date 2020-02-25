package org.dataaligorithms.searchengine.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:该买过该商品的用户还购买了哪些商品(CWBTIAB)MapReduce实现，使用Stripes设计模式，第一阶段
 * reducer，对输入输出不做处理
 * @Author: FanYueXiang
 * @Date: 2020/2/25 9:28 PM
 */
public class CWBTIABReducerStep1 extends Reducer<Text, Text, Text, Iterable<Text>> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, values);
    }

}
