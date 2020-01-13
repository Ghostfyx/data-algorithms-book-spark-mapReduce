package org.dataalgorithms.topn.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-13
 */
public class AggregateByKeyDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage AggregateByKeyDriver <input> <output>");
            System.exit(1);
        }

        System.out.println("inputDir="+args[0]);
        System.out.println("outputDir="+args[1]);
        int returnStatus = ToolRunner.run(new AggregateByKeyDriver(), args);
        System.exit(returnStatus);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(AggregateByKeyDriver.class);
        job.setJobName("AggregateByKeyDriver");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Mapper与Reducer 输出的<key,value>类型一样
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(AggregateByKeyMapper.class);
        // combiner 实现本地key的聚合，topN中由于key不唯一，因此Combiner与Reducer是同一个类，不同的是
        // combiner实现的是本地聚合
        job.setReducerClass(AggregateByKeyReducer.class);
        job.setCombinerClass(AggregateByKeyReducer.class);

        // args[0] = input directory
        // args[1] = output directory
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
       System.out.println("run(): status="+status);
        return status ? 0 : 1;
    }

}
