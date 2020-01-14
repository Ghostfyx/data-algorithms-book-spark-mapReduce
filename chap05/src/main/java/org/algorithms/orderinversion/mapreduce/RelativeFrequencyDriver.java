package org.algorithms.orderinversion.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @description: 按照词向量出现相对频率反转排序
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class RelativeFrequencyDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("usage:<input> <output> <window>");
            System.exit(-1);
        }
        //
        int status = ToolRunner.run(new RelativeFrequencyDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(RelativeFrequencyDriver.class);
        job.setJobName("RelativeFrequency");

        String inputPath = args[0];
        String outputPath = args[1];
        String neighborWindow = args[2];
        job.getConfiguration().setInt("neighbor.window", Integer.parseInt(neighborWindow));
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setPartitionerClass(OrderInversionPartitioner.class);
        job.setCombinerClass(OrderInversionCombiner.class);
        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(PairOfWords.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfWords.class);
        job.setOutputValueClass(DoubleWritable.class);
        boolean status = job.waitForCompletion(true);
        System.out.println("run(): status="+status);
        return status ? 0 : 1;
    }
}
