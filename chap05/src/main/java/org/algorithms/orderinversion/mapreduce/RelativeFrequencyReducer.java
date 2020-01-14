package org.algorithms.orderinversion.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description: 词向量相对频度归约器，归约器输入之前已经本地Combine过了
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class RelativeFrequencyReducer extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {

    private double totalCount = 0;
    private final DoubleWritable relativeCount = new DoubleWritable();
    private String currentWord = "NOT_DEFINED";

    /**
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    public void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if ("*".equals(key.getRightWord())){
            // {[(W_1, *),n_1],[(W_1, *), n_2]} 应对这样的情况出现
            if (key.getLeftWord().equals(currentWord)){
                System.out.println("{[(W_1, *),n_1],[(W_1, *), n_2]}");
                totalCount += getTotalCount(values);
            }else {
                currentWord = key.getLeftWord();
                totalCount = getTotalCount(values);
            }
        } else{
            // 由于是已经combine过后的数据
            int count = getTotalCount(values);
            relativeCount.set((double) count / totalCount);
            context.write(key, relativeCount);
        }
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }

}
