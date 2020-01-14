package org.algorithms.orderinversion.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description: 文档相对词频倒转排序Mapper
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable> {

    private int neighborWindow = 2;
    // pair = (leftElement, rightElement)
    private final PairOfWords pair = new PairOfWords();
    private final IntWritable totalCount = new IntWritable();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        if ((tokens == null) || (tokens.length < 2)) {
            return;
        }
        for (int i = 0; i < tokens.length; i++){
            // replaceAll regex的用法见第五章对应笔记
            tokens[i] = tokens[i].replaceAll("\\W+", "");
            if (tokens[i].equals("")) {
                continue;
            }
            pair.setWord(tokens[i]);
            // 获取当前元素的左右窗口
            int start = (i - neighborWindow < 0) ? 0 : i-neighborWindow;
            int end = (i + neighborWindow >= tokens.length - 1) ? tokens.length - 1 : i + neighborWindow;
            for (int j = start; j <= end; j++){
//                当前元素左右窗口包含的元素 (W_i, W_start),(W_i, W_start+1),....,(W_i, W_end)
                if (j == i) {
                    continue;
                }
                pair.setNeighbor(tokens[j].replaceAll("\\W", ""));
                context.write(pair, ONE);
            }
            // 遍历完当前元素，输出总结性数据
            pair.setNeighbor("*");
            totalCount.set(end-start);
            context.write(pair, totalCount);
        }

    }

    @Override
    public void setup(Context context){
        this.neighborWindow = context.getConfiguration().getInt("neighbor.window",2);
    }

}
