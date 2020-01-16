package org.dataalgorithms.MBA.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-16
 */
public class MBADriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("USAGE: [input-path] [output-path] [number-of-pairs]");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int numberOfPairs = Integer.parseInt(args[2]);

        Job job = new Job(getConf());
        job.setJobName("MBADriver");
        job.setJarByClass(MBADriver.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.getConfiguration().setInt("number.of.pairs", numberOfPairs);
        job.setMapperClass(MBAMapper.class);
        job.setReducerClass(MBAReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitStatus = ToolRunner.run(new MBADriver(), args);
        System.exit(exitStatus);
    }
}
