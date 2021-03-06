package org.dataalgorithms.leftjoin.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class LeftJoinDriver {

    public static void main(String[] args) throws Exception {
        Path transactions = new Path(args[0]);
        Path users = new Path(args[1]);
        Path output = new Path(args[2]);

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJobName("LeftJoin_firstStep");
        job.setJarByClass(LeftJoinDriver.class);

        // "secondary sort" is handled by setting the following 3 plug-ins:
        // 1. how the mapper generated keys will be partitioned
        job.setPartitionerClass(SecondarySortPartitioner.class);
        /**2. how the natural keys (generated by mappers) will be grouped
         *  每一个Reducer（归约器）接收一个分片，Reducer可能会执行多次reduce方法，reduce方法里，接收的参数中，value是一个迭代的值，框架把“key 相同”的k-v的v值，放在一个迭代器里
         *  key是否相同的过程叫做分组
         */
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
        job.setReducerClass(LeftJoinReducer.class);

        // set reducer output data type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // define multiple mappers，如果hadoop MR计算需要多个Mapper则这么定义
        MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, LeftJoinTransactionMapper.class);
        MultipleInputs.addInputPath(job, users, TextInputFormat.class, LeftJoinUserMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        FileOutputFormat.setOutputPath(job, output);
        if (job.waitForCompletion(true)) {
            return;
        }
        else {
            throw new Exception("Phase-1: Left Outer Join Job Failed");
        }
    }

}
