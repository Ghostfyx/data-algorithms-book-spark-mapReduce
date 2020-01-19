package org.dataalgorithms.commonfriend.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * @createDate: 2020-01-19
 */
public class CommonFriendsDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(CommonFriendsDriver.class);
        job.setJobName("CommonFriends");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);			// mapper will generate key as Text (the keys are as (person1,person2))
        job.setOutputValueClass(Text.class);		// mapper will generate value as Text (list of friends)

        job.setMapperClass(CommonFriendsMapper.class);
        job.setReducerClass(CommonFriendsReducer.class);

        // args[0] = input directory
        // args[1] = output directory
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        System.out.println("run(): status=" + status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 2 parameters
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: Argument 1: input dir, Argument 2: output dir");
        }

        System.out.println("inputDir=" + args[0]);
        System.out.println("outputDir=" + args[1]);
        int jobStatus =  ToolRunner.run(new CommonFriendsDriver(), args);
        System.out.println("jobStatus=" + jobStatus);
        System.exit(jobStatus);
    }

}
