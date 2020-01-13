package org.dataalgorithms.topn.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-10 9:14
 */
public class TopNDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        int N = Integer.parseInt(args[0]);
        // 将参数设置在Hadoop配置中
        job.getConfiguration().setInt("N", N);
        job.setJobName("TopNDriverUseMR");
        job.setJarByClass(TopNDriver.class);

        // 使用SequenceFile读取写入文件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        // 设置只有一个Reducer，将所有Mapper结果映射到一个Reducer执行
        job.setNumReduceTasks(1);

        // map()'s output (K,V)
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // reduce()'s output (K,V)
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        // args[1] = input directory
        // args[2] = output directory
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean status = job.waitForCompletion(true);
        System.out.println("run(): status="+status);
        return status ? 0 : 1;
    }

    /**
     * The main driver for "Top N" program.
     * Invoke this method to submit the map/reduce job.
     * @throws Exception When there is communication problems with the job tracker.
     */
    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 3 parameters
        if (args.length != 3) {
            System.out.println("usage TopNDriver <N> <input> <output>");
            System.exit(1);
        }

        System.out.println("N="+args[0]);
        System.out.println("inputDir="+args[1]);
        System.out.println("outputDir="+args[2]);
        int returnStatus = ToolRunner.run(new TopNDriver(), args);
        System.exit(returnStatus);
    }
}
